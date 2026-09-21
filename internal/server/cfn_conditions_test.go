package server

import (
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkcfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"

	"github.com/sivchari/kumo/internal/service/cloudformation"
)

const conditionalTemplate = `{"Parameters":{"Enabled":{"Type":"String"},"Zero":{"Type":"String"},"Code":{"Type":"String"},"Defaulted":{"Type":"String","Default":"kept"}},"Conditions":{"On":{"Fn::Equals":[{"Ref":"Enabled"},"true"]},"Off":{"Fn::Not":[{"Condition":"On"}]},"Both":{"Fn::And":[{"Condition":"On"},{"Fn::Equals":[{"Ref":"Zero"},"0"]}]},"Either":{"Fn::Or":[{"Condition":"Both"},{"Condition":"Off"}]}},"Resources":{"Conditional":{"Type":"AWS::S3::Bucket","Condition":"On","Properties":{}},"Always":{"Type":"AWS::S3::Bucket","Condition":"Either","Properties":{"Choice":{"Fn::If":["On","yes","no"]},"Optional":{"Fn::If":["On","included",{"Ref":"AWS::NoValue"}]},"Values":["first",{"Fn::If":["Off",{"Ref":"AWS::NoValue"},"second"]}],"Code":{"Ref":"Code"},"Default":{"Ref":"Defaulted"}}}}}`

// SDK encoding and the real dispatcher must preserve strings before evaluating conditions.
func TestCloudFormationSDKConditionalParameters(t *testing.T) {
	t.Parallel()

	for _, enabled := range []string{"false", "true"} {
		t.Run(enabled, func(t *testing.T) {
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
				StackName: aws.String("conditions"), TemplateBody: aws.String(conditionalTemplate),
				Parameters: []types.Parameter{
					{ParameterKey: aws.String("Enabled"), ParameterValue: aws.String(enabled)},
					{ParameterKey: aws.String("Zero"), ParameterValue: aws.String("0")},
					{ParameterKey: aws.String("Code"), ParameterValue: aws.String("0012")},
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			assertConditionalStack(t, store, enabled)
		})
	}
}

// Assert materialized properties independently of the evaluator's intermediate representation.
func assertConditionalStack(t *testing.T, store *cloudformation.MemoryStorage, enabled string) {
	t.Helper()

	stacks, err := store.DescribeStacks(t.Context(), "conditions")
	if err != nil || len(stacks) != 1 {
		t.Fatalf("DescribeStacks: %v count=%d", err, len(stacks))
	}

	wantParameters := map[string]string{"Enabled": enabled, "Zero": "0", "Code": "0012", "Defaulted": "kept"}
	if !reflect.DeepEqual(stacks[0].Parameters, wantParameters) {
		t.Fatalf("parameters=%v", stacks[0].Parameters)
	}

	wantCount := 1
	want := map[string]any{"Choice": "no", "Values": []any{"first"}, "Code": "0012", "Default": "kept"}

	if enabled == "true" {
		wantCount = 2
		want["Choice"] = "yes"
		want["Optional"] = "included"
		want["Values"] = []any{"first", "second"}
	}

	resources, err := store.DescribeStackResources(t.Context(), "conditions", "")
	if err != nil || len(resources) != wantCount {
		t.Fatalf("resources: %v count=%d want=%d", err, len(resources), wantCount)
	}

	found := false

	for _, resource := range resources {
		if resource.LogicalResourceID == "Always" {
			found = true

			if !reflect.DeepEqual(resource.Properties, want) {
				t.Fatalf("properties=%#v want=%#v", resource.Properties, want)
			}
		}
	}

	if !found {
		t.Fatal("Always resource missing")
	}

	original, err := store.GetTemplate(t.Context(), "conditions")
	if err != nil || original != conditionalTemplate {
		t.Fatalf("original template changed: %v %s", err, original)
	}
}
