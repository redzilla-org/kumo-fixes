package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkcfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"

	"github.com/sivchari/kumo/internal/service/cloudformation"
)

// The SDK must pass through the real Query decoder, not a pre-decoded test body.
func TestCloudFormationSDKParameterArray(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewMemoryStorage()
	svc := cloudformation.New(store)
	dispatcher := NewQueryProtocolDispatcher()
	dispatcher.RegisterAction("CreateStack", svc.TargetPrefix(), svc.ServiceIdentifier(), svc.DispatchAction)
	host := httptest.NewServer(dispatcher)
	t.Cleanup(host.Close)

	client := sdkcfn.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}, func(o *sdkcfn.Options) {
		o.BaseEndpoint = aws.String(host.URL)
	})

	_, err := client.CreateStack(t.Context(), &sdkcfn.CreateStackInput{
		StackName: aws.String("sdk-parameters"), TemplateBody: aws.String(`{"Resources":{}}`),
		Parameters: []types.Parameter{{ParameterKey: aws.String("Stage"), ParameterValue: aws.String("testing")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	stacks, err := store.DescribeStacks(t.Context(), "sdk-parameters")
	if err != nil || len(stacks) != 1 {
		t.Fatalf("DescribeStacks: %v, count=%d", err, len(stacks))
	}

	if stacks[0].Parameters["Stage"] != "testing" {
		t.Fatalf("parameters=%v", stacks[0].Parameters)
	}

	// Existing callers with keyed JSON parameters must retain their supported shape.
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(`{"StackName":"json-parameters","TemplateBody":"{\"Resources\":{}}","Parameters":{"Stage":"direct"}}`))
	w := httptest.NewRecorder()
	svc.CreateStack(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("direct JSON: %d %s", w.Code, w.Body.String())
	}

	stacks, err = store.DescribeStacks(t.Context(), "json-parameters")
	if err != nil || len(stacks) != 1 || stacks[0].Parameters["Stage"] != "direct" {
		t.Fatalf("direct JSON parameters: %v %v", stacks, err)
	}
}
