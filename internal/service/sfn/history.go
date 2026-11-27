package sfn

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// HistoryDetails keeps the AWS event-specific wire shapes with each real
// transition, including the resolved task Parameters of each attempt.
type HistoryDetails struct {
	StateEnteredEventDetails            map[string]any `json:"stateEnteredEventDetails,omitempty"`
	StateExitedEventDetails             map[string]any `json:"stateExitedEventDetails,omitempty"`
	TaskScheduledEventDetails           map[string]any `json:"taskScheduledEventDetails,omitempty"`
	TaskStartedEventDetails             map[string]any `json:"taskStartedEventDetails,omitempty"`
	TaskSucceededEventDetails           map[string]any `json:"taskSucceededEventDetails,omitempty"`
	TaskFailedEventDetails              map[string]any `json:"taskFailedEventDetails,omitempty"`
	TaskTimedOutEventDetails            map[string]any `json:"taskTimedOutEventDetails,omitempty"`
	LambdaFunctionScheduledEventDetails map[string]any `json:"lambdaFunctionScheduledEventDetails,omitempty"`
	LambdaFunctionSucceededEventDetails map[string]any `json:"lambdaFunctionSucceededEventDetails,omitempty"`
	LambdaFunctionFailedEventDetails    map[string]any `json:"lambdaFunctionFailedEventDetails,omitempty"`
	LambdaFunctionTimedOutEventDetails  map[string]any `json:"lambdaFunctionTimedOutEventDetails,omitempty"`
}

// Each branch owns a cursor while storage serializes event IDs for the whole
// execution. Forking the cursor avoids racing on Parallel/Map transitions.
type historyContextKey struct{}

// executionContextKey carries the ASL Context object ($$.Execution, $$.State, ...).
// Values are copied for state/callback additions so Parallel branches do not share writes.
type executionContextKey struct{}

func contextData(ctx context.Context) map[string]any {
	data, _ := ctx.Value(executionContextKey{}).(map[string]any)
	copy := make(map[string]any, len(data)+1)
	for key, value := range data {
		copy[key] = value
	}
	return copy
}

type historyCursor struct {
	last   int64
	region string
	append func(*HistoryEvent) int64
}

func forkHistory(ctx context.Context) context.Context {
	if cursor, ok := ctx.Value(historyContextKey{}).(*historyCursor); ok {
		copy := *cursor
		return context.WithValue(ctx, historyContextKey{}, &copy)
	}
	return ctx
}

func recordHistory(ctx context.Context, eventType string, details HistoryDetails) {
	if cursor, ok := ctx.Value(historyContextKey{}).(*historyCursor); ok {
		cursor.last = cursor.append(&HistoryEvent{
			Timestamp: time.Now(), Type: HistoryEventType(eventType),
			PreviousEventID: cursor.last, HistoryDetails: details,
		})
	}
}

// appendHistory stops accepting events after a terminal transition, so a
// canceled HTTP response cannot append a success behind ExecutionAborted.
func (s *MemoryStorage) appendHistory(ed *ExecutionData, event *HistoryEvent) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ed.Execution.Status != ExecutionStatusRunning {
		return int64(len(ed.History))
	}
	event.ID = int64(len(ed.History)) + 1
	ed.History = append(ed.History, event)
	s.saveLocked()
	return event.ID
}

// recordTaskAttempt surrounds each real attempt, so retries retain separate
// scheduled parameters and failure events rather than only the final result.
func recordTaskAttempt(ctx context.Context, state *stateDefinition, params map[string]any, input string, invoke func() (string, error)) (string, error) {
	direct := strings.HasPrefix(state.Resource, "arn:aws:lambda:")
	resourceType, resource := "", state.Resource
	if prefix, suffix, ok := strings.Cut(strings.TrimPrefix(resource, "arn:aws:states:::"), ":"); ok {
		resourceType, resource = prefix, suffix
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("encode task history: %w", err)
	}
	if direct {
		payload := input
		if len(params) != 0 {
			payload = string(encoded)
		}
		detail := map[string]any{"resource": state.Resource, "input": payload}
		if state.TimeoutSeconds != nil {
			detail["timeoutInSeconds"] = *state.TimeoutSeconds
		}
		recordHistory(ctx, "LambdaFunctionScheduled", HistoryDetails{LambdaFunctionScheduledEventDetails: detail})
		recordHistory(ctx, "LambdaFunctionStarted", HistoryDetails{})
	} else {
		detail := map[string]any{"resourceType": resourceType, "resource": resource, "parameters": string(encoded)}
		if cursor, ok := ctx.Value(historyContextKey{}).(*historyCursor); ok {
			detail["region"] = cursor.region
		}
		if state.TimeoutSeconds != nil {
			detail["timeoutInSeconds"] = *state.TimeoutSeconds
		}
		recordHistory(ctx, "TaskScheduled", HistoryDetails{TaskScheduledEventDetails: detail})
		recordHistory(ctx, "TaskStarted", HistoryDetails{TaskStartedEventDetails: map[string]any{"resourceType": resourceType, "resource": resource}})
	}
	output, err := invoke()
	if err == nil {
		if direct {
			recordHistory(ctx, "LambdaFunctionSucceeded", HistoryDetails{LambdaFunctionSucceededEventDetails: map[string]any{"output": output}})
		} else {
			recordHistory(ctx, "TaskSucceeded", HistoryDetails{TaskSucceededEventDetails: map[string]any{"resourceType": resourceType, "resource": resource, "output": output}})
		}
		return output, nil
	}
	detail := map[string]any{"error": executionErrorCode(err), "cause": executionErrorCause(err)}
	timedOut := ctx.Err() == context.DeadlineExceeded || executionErrorCode(err) == errorStatesTimeout
	if timedOut {
		detail["error"] = errorStatesTimeout
	}
	if direct {
		if timedOut {
			recordHistory(ctx, "LambdaFunctionTimedOut", HistoryDetails{LambdaFunctionTimedOutEventDetails: detail})
		} else {
			recordHistory(ctx, "LambdaFunctionFailed", HistoryDetails{LambdaFunctionFailedEventDetails: detail})
		}
	} else {
		detail["resourceType"], detail["resource"] = resourceType, resource
		if timedOut {
			recordHistory(ctx, "TaskTimedOut", HistoryDetails{TaskTimedOutEventDetails: detail})
		} else {
			recordHistory(ctx, "TaskFailed", HistoryDetails{TaskFailedEventDetails: detail})
		}
	}
	return "", err
}

// Tokens carry the execution and snapshot size as well as the offset, so reverse
// pagination remains stable while a running execution appends further events.
type historyPage struct {
	Execution string `json:"execution"`
	Offset    int    `json:"offset"`
	Size      int    `json:"size"`
	Reverse   bool   `json:"reverse"`
}

func historyPageToken(page historyPage) string {
	encoded, _ := json.Marshal(page)
	return base64.RawURLEncoding.EncodeToString(encoded)
}
