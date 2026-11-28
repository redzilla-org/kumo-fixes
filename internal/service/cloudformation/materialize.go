package cloudformation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/dynamodb"
	"github.com/sivchari/kumo/internal/service/ssm"
)

const tableResource = "AWS::DynamoDB::Table"
const parameterResource = "AWS::SSM::Parameter"

// Materialization marker values recorded on each StackResource.
const (
	// MaterializationReal means the resource exists in the backing service store.
	MaterializationReal = "MATERIALIZED"
	// MaterializationInert means the resource is a stack record only; the marker
	// makes that visible instead of reporting it as silently created.
	MaterializationInert = "TRACKED_INERT"
)

// Types that are only tracked as stack records: nothing in the emulator enforces them.
var inertResourceTypes = map[string]bool{
	"AWS::IAM::Role":          true,
	"AWS::IAM::ManagedPolicy": true,
	"AWS::Glue::Database":     true,
	"AWS::Athena::WorkGroup":  true,
	"AWS::CDK::Metadata":      true,
}

// A stack that reports CREATE_COMPLETE over resources that were never created is
// worse than a failed call, so every other type is rejected before anything is created.
func checkResourceTypes(stack *Stack) error {
	for _, resource := range stack.Resources {
		switch {
		case resource.ResourceType == bucketResource, resource.ResourceType == policyResource,
			resource.ResourceType == tableResource, resource.ResourceType == parameterResource:
		case inertResourceTypes[resource.ResourceType]:
		default:
			return fmt.Errorf("resource %s has type %q, which this emulator cannot create", resource.LogicalResourceID, resource.ResourceType)
		}
	}

	return nil
}

// Inert resources get a recognizable physical ID before anything Refs them.
func markInertResources(stack *Stack) {
	for i := range stack.Resources {
		resource := &stack.Resources[i]
		if inertResourceTypes[resource.ResourceType] {
			resource.Materialization = MaterializationInert
			resource.PhysicalResourceID = resource.LogicalResourceID + "-inert"
		}
	}
}

type stackRegionKey struct{}

// The storage layer has no request, so the handler passes the region through the context.
func withStackRegion(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, stackRegionKey{}, region)
}

func stackRegion(ctx context.Context) string {
	if region, ok := ctx.Value(stackRegionKey{}).(string); ok && region != "" {
		return region
	}

	return "us-east-1"
}

// regionFromAuthHeader extracts the signing region from a SigV4 Authorization header
// (Credential=AKID/20260101/us-west-2/cloudformation/aws4_request). Empty when absent.
func regionFromAuthHeader(auth string) string {
	i := strings.Index(auth, "Credential=")
	if i < 0 {
		return ""
	}

	parts := strings.Split(strings.SplitN(auth[i+len("Credential="):], ",", 2)[0], "/")
	if len(parts) >= 3 {
		return parts[2]
	}

	return ""
}

// Precedence follows the other services, which read AWS_DEFAULT_REGION: the
// configured region, then the request's signing region, then us-east-1.
func resolveRegion(r *http.Request) string {
	if v := os.Getenv("AWS_DEFAULT_REGION"); v != "" {
		return v
	}

	if v := regionFromAuthHeader(r.Header.Get("Authorization")); v != "" {
		return v
	}

	return "us-east-1"
}

// Resolve the registered stores at execution time, after all service initializers run.
func stackDynamoDBStorage() (dynamodb.Storage, error) {
	if svc, ok := service.Get("dynamodb"); ok {
		if d, ok := svc.(*dynamodb.Service); ok {
			return d.Storage(), nil
		}
	}

	return nil, errors.New("DynamoDB service is not registered")
}

func stackSSMStorage() (ssm.Storage, error) {
	if svc, ok := service.Get("ssm"); ok {
		if s, ok := svc.(*ssm.Service); ok {
			return s.Storage(), nil
		}
	}

	return nil, errors.New("SSM service is not registered")
}

// Runs after provisionStackS3, so Refs to buckets resolve to their real names.
// Tables go before parameters because a parameter value commonly Refs a table.
// Partial creations are undone here; the caller undoes the stack's S3 resources.
func provisionStackResources(ctx context.Context, stack *Stack) (err error) {
	for i := range stack.Resources {
		resource := &stack.Resources[i]
		if resource.ResourceType == bucketResource || resource.ResourceType == policyResource {
			resource.Materialization = MaterializationReal
		}
	}

	needed := false
	for _, resource := range stack.Resources {
		if resource.ResourceType == tableResource || resource.ResourceType == parameterResource {
			needed = true
		}
	}

	if !needed {
		return nil
	}

	var template s3StackTemplate
	if err := json.Unmarshal([]byte(stack.TemplateBody), &template); err != nil {
		return fmt.Errorf("parse stack template: %w", err)
	}

	tables, err := stackDynamoDBStorage()
	if err != nil {
		return err
	}

	params, err := stackSSMStorage()
	if err != nil {
		return err
	}

	refs := map[string]string{"AWS::StackName": stack.StackName, "AWS::StackId": stack.StackID, "AWS::Region": stackRegion(ctx), "AWS::AccountId": "123456789012", "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com"}
	for name, parameter := range template.Parameters {
		if parameter.Default != nil {
			refs[name] = fmt.Sprint(parameter.Default)
		}
	}

	for name, value := range stack.Parameters {
		refs[name] = value
	}

	for _, resource := range stack.Resources {
		if resource.ResourceType != tableResource && resource.ResourceType != parameterResource {
			refs[resource.LogicalResourceID] = resource.PhysicalResourceID
		}
	}

	var undo []func() error
	defer func() {
		if err != nil {
			for i := len(undo) - 1; i >= 0; i-- {
				err = errors.Join(err, undo[i]())
			}
		}
	}()

	for _, kind := range []string{tableResource, parameterResource} {
		for i := range stack.Resources {
			resource := &stack.Resources[i]
			if resource.ResourceType != kind {
				continue
			}

			resolved, resolveErr := stackValue(map[string]any(template.Resources[resource.LogicalResourceID].Properties), refs)
			if resolveErr != nil {
				return fmt.Errorf("resource %s: %w", resource.LogicalResourceID, resolveErr)
			}

			properties, _ := resolved.(map[string]any)

			var name string
			if kind == tableResource {
				name, err = createStackTable(ctx, tables, properties)
			} else {
				name, err = createStackParameter(ctx, params, properties)
			}

			if err != nil {
				return fmt.Errorf("resource %s: %w", resource.LogicalResourceID, err)
			}

			if kind == tableResource {
				undo = append(undo, func() error { _, err := tables.DeleteTable(ctx, name); return err })
			} else {
				undo = append(undo, func() error { return params.DeleteParameter(ctx, name) })
			}

			resource.PhysicalResourceID = name
			resource.Materialization = MaterializationReal
			refs[resource.LogicalResourceID] = name
		}
	}

	return nil
}

// The CFN property names used here match the DynamoDB API JSON, so a JSON round trip is the mapping.
func createStackTable(ctx context.Context, tables dynamodb.Storage, properties map[string]any) (string, error) {
	raw, err := json.Marshal(properties)
	if err != nil {
		return "", fmt.Errorf("marshal table properties: %w", err)
	}

	var req dynamodb.CreateTableRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return "", fmt.Errorf("map table properties: %w", err)
	}

	if req.TableName == "" {
		return "", errors.New("TableName is required; generated table names are not supported")
	}

	if _, err := tables.CreateTable(ctx, &req); err != nil {
		return "", err
	}

	// TTL is a separate call on the real API as well.
	if ttl, ok := properties["TimeToLiveSpecification"].(map[string]any); ok {
		attribute, _ := ttl["AttributeName"].(string)
		enabled, _ := ttl["Enabled"].(bool)

		if err := tables.UpdateTimeToLive(ctx, req.TableName, attribute, enabled); err != nil {
			_, _ = tables.DeleteTable(ctx, req.TableName)

			return "", err
		}
	}

	return req.TableName, nil
}

// The CFN property names used here match the SSM PutParameter JSON.
func createStackParameter(ctx context.Context, params ssm.Storage, properties map[string]any) (string, error) {
	raw, err := json.Marshal(properties)
	if err != nil {
		return "", fmt.Errorf("marshal parameter properties: %w", err)
	}

	var req ssm.PutParameterRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return "", fmt.Errorf("map parameter properties: %w", err)
	}

	if req.Name == "" || req.Value == "" {
		return "", errors.New("SSM parameter requires Name and Value")
	}

	// The stack owns the parameter, so an existing value is replaced.
	req.Overwrite = true

	if _, err := params.PutParameter(ctx, &req); err != nil {
		return "", err
	}

	return req.Name, nil
}

// Only materialized tables and parameters are removed, parameters first; any failure
// fails DeleteStack so nothing leaks silently.
func deleteStackResources(ctx context.Context, stack *Stack) error {
	var errs []error

	for _, kind := range []string{parameterResource, tableResource} {
		for _, resource := range stack.Resources {
			if resource.ResourceType != kind || resource.Materialization != MaterializationReal {
				continue
			}

			if kind == parameterResource {
				params, err := stackSSMStorage()
				if err != nil {
					return err
				}

				if err := params.DeleteParameter(ctx, resource.PhysicalResourceID); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", resource.LogicalResourceID, err))
				}

				continue
			}

			tables, err := stackDynamoDBStorage()
			if err != nil {
				return err
			}

			if _, err := tables.DeleteTable(ctx, resource.PhysicalResourceID); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", resource.LogicalResourceID, err))
			}
		}
	}

	return errors.Join(errs...)
}
