package sfn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sivchari/kumo/internal/service/lambda"
)

// Exercise the real Lambda broker and Runtime API, not an HTTP stub
// returning a hand-written Invoke envelope. The sample handler totals its input.
func TestLambdaWorkflowPreservesContextAndOrderedResults(t *testing.T) {
	lambdaStore := lambda.NewMemoryStorage("http://localhost")
	_, err := lambdaStore.CreateFunction(t.Context(), &lambda.CreateFunctionRequest{
		FunctionName: "sum", Runtime: "provided.al2023", Handler: "bootstrap",
		Role: "arn:aws:iam::123456789012:role/build", Timeout: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := lambda.New(lambdaStore, "http://localhost")
	t.Cleanup(func() { _ = svc.Close() })
	server := httptest.NewServer(http.HandlerFunc(svc.Invoke))
	t.Cleanup(server.Close)
	const runtimePath = "/_runtime/sum/2018-06-01/runtime/invocation/"
	// A canceled idle poll registers the runtime before work is submitted.
	pollCtx, cancel := context.WithCancel(t.Context())
	cancel()
	svc.RuntimeNext(httptest.NewRecorder(), httptest.NewRequestWithContext(pollCtx, http.MethodGet, runtimePath+"next", nil))
	handlerDone := make(chan error, 1)
	go func() {
		next := httptest.NewRecorder()
		svc.RuntimeNext(next, httptest.NewRequestWithContext(t.Context(), http.MethodGet, runtimePath+"next", nil))
		var event struct {
			Numbers []int  `json:"numbers"`
			Run     string `json:"run"`
			State   string `json:"state"`
		}
		if err := json.Unmarshal(next.Body.Bytes(), &event); err != nil {
			handlerDone <- err
			return
		}
		total := 0
		for _, value := range event.Numbers {
			total += value
		}
		response, err := json.Marshal(map[string]any{"total": total, "run": event.Run, "state": event.State})
		if err != nil {
			handlerDone <- err
			return
		}
		svc.RuntimeResponse(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost,
			runtimePath+next.Header().Get("Lambda-Runtime-Aws-Request-Id")+"/response", strings.NewReader(string(response))))
		handlerDone <- nil
	}()
	store := NewMemoryStorage(WithBaseURL(server.URL))
	definition := `{"StartAt":"Builds","States":{
		"Builds":{"Type":"Parallel","Branches":[
			{"StartAt":"Web","States":{"Web":{"Type":"Pass","Result":{"size":7},"End":true}}},
			{"StartAt":"API","States":{"API":{"Type":"Pass","Result":{"size":3},"End":true}}}
		],"ResultSelector":{"web.$":"$[0].size","api.$":"$[1].size"},"Next":"Prepare"},
		"Prepare":{"Type":"Pass","Parameters":{
			"total.$":"States.MathAdd($.web, $.api)",
			"decoded.$":"States.StringToJson('{\"numbers\":[7,3]}')",
			"label.$":"States.Format('build-{}', States.MathAdd($.web, $.api))"
		},"Next":"Invoke"},
		"Invoke":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke",
			"Parameters":{"FunctionName":"sum","Payload":{"numbers.$":"$.decoded.numbers","run.$":"$$.Execution.Id","state.$":"$$.State.Name"}},
			"ResultSelector":{"total.$":"$.Payload.total","run.$":"$.Payload.run","state.$":"$.Payload.state"},"ResultPath":"$.lambda","End":true}
	}}`
	sm := createExecutionTestStateMachine(t, store, "build-results", definition)
	exec := startAndAwaitSuccess(t, store, sm.StateMachineArn, `{}`)
	if err := <-handlerDone; err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(map[string]any{
		"total": 10, "decoded": map[string]any{"numbers": []int{7, 3}}, "label": "build-10",
		"lambda": map[string]any{"total": 10, "run": exec.ExecutionArn, "state": "Invoke"},
	})
	if err != nil || !jsonDeepEqual(exec.Output, string(want)) {
		t.Fatalf("workflow output=%s, want=%s, marshal=%v", exec.Output, want, err)
	}
	// AWS history orders each execution independently; pagination must retain
	// the resolved scheduling payload used to explain a real build invocation.
	history, _, err := store.GetExecutionHistory(t.Context(), exec.ExecutionArn, 100, "", false)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled map[string]any
	for i, event := range history {
		if event.ID != int64(i+1) || event.PreviousEventID >= event.ID {
			t.Fatalf("history event %d: id=%d previous=%d", i, event.ID, event.PreviousEventID)
		}
		if event.Type == "TaskScheduled" {
			scheduled = event.TaskScheduledEventDetails
		}
	}
	var params map[string]any
	if scheduled == nil || json.Unmarshal([]byte(scheduled["parameters"].(string)), &params) != nil {
		t.Fatalf("missing scheduling parameters: %v", scheduled)
	}
	payload := params["Payload"].(map[string]any)
	if params["FunctionName"] != "sum" || payload["run"] != exec.ExecutionArn || payload["state"] != "Invoke" {
		t.Fatalf("scheduled parameters=%v", params)
	}
	for _, reverse := range []bool{false, true} {
		var ids []int64
		token := ""
		for {
			page, next, err := store.GetExecutionHistory(t.Context(), exec.ExecutionArn, 3, token, reverse)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range page {
				ids = append(ids, event.ID)
			}
			if next == "" {
				break
			}
			token = next
		}
		wantIDs := make([]int64, len(history))
		for i := range wantIDs {
			wantIDs[i] = int64(i + 1)
			if reverse {
				wantIDs[i] = int64(len(history) - i)
			}
		}
		if !reflect.DeepEqual(ids, wantIDs) {
			t.Fatalf("reverse=%v paged IDs=%v, want %v", reverse, ids, wantIDs)
		}
	}
}

// Updating a deployed definition must not change an in-flight
// execution, and StopExecution must survive completion of its former Wait.
func TestUpdateAndStopKeepExecutionIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemoryStorage()
		old := `{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":5,"Next":"Done"},"Done":{"Type":"Pass","Result":{"revision":"old"},"End":true}}}`
		sm := createExecutionTestStateMachine(t, store, "update-in-flight", old)
		started, err := store.StartExecution(t.Context(), sm.StateMachineArn, "in-flight", `{}`, "")
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		_, err = store.UpdateStateMachine(t.Context(), &UpdateStateMachineRequest{
			StateMachineArn: sm.StateMachineArn,
			Definition:      `{"StartAt":"Done","States":{"Done":{"Type":"Pass","Result":{"revision":"new"},"End":true}}}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		latest := startAndAwaitSuccess(t, store, sm.StateMachineArn, `{}`)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		prior, err := store.DescribeExecution(t.Context(), started.ExecutionArn)
		if err != nil || prior.Status != ExecutionStatusSucceeded || prior.Output != `{"revision":"old"}` || latest.Output != `{"revision":"new"}` {
			t.Fatalf("old=%+v new=%+v err=%v", prior, latest, err)
		}
		// Both completed executions must begin at event one, not a global counter.
		for _, execution := range []*Execution{prior, latest} {
			events, _, err := store.GetExecutionHistory(t.Context(), execution.ExecutionArn, 1, "", false)
			if err != nil || len(events) != 1 || events[0].ID != 1 || events[0].Type != HistoryEventTypeExecutionStarted {
				t.Fatalf("execution=%s first history=%+v err=%v", execution.ExecutionArn, events, err)
			}
		}
		waiter := createExecutionTestStateMachine(t, store, "stop-in-flight", old)
		stopped, err := store.StartExecution(t.Context(), waiter.StateMachineArn, "user-stop", `{}`, "")
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if _, err := store.StopExecution(t.Context(), stopped.ExecutionArn, "UserStopped", "build superseded"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(6 * time.Second)
		synctest.Wait()
		stopped, err = store.DescribeExecution(t.Context(), stopped.ExecutionArn)
		if err != nil || stopped.Status != ExecutionStatusAborted || stopped.Error != "UserStopped" || stopped.Cause != "build superseded" {
			t.Fatalf("stopped=%+v err=%v", stopped, err)
		}
		events, _, err := store.GetExecutionHistory(t.Context(), stopped.ExecutionArn, 100, "", false)
		if err != nil || events[len(events)-1].Type != HistoryEventTypeExecutionAborted {
			t.Fatalf("history after StopExecution=%+v err=%v", events, err)
		}
	})
}
