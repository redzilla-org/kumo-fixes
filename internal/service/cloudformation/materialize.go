package cloudformation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/dynamodb"
	"github.com/sivchari/kumo/internal/service/ssm"
)

const tableResource = "AWS::DynamoDB::Table"
const parameterResource = "AWS::SSM::Parameter"

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
// On update, table and parameter definitions must be unchanged: applying a diff
// is not supported, and silently keeping the old resource would hide that.
func provisionStackResources(ctx context.Context, stack *Stack, old *Stack) (err error) {
	needed := false
	for _, resource := range stack.Resources {
		if resource.ResourceType == tableResource || resource.ResourceType == parameterResource {
			needed = true
		}
	}

	if old != nil {
		for _, resource := range old.Resources {
			if resource.ResourceType == tableResource || resource.ResourceType == parameterResource {
				needed = true
			}
		}
	}

	if !needed {
		return nil
	}

	var template s3StackTemplate
	if err := json.Unmarshal([]byte(stack.TemplateBody), &template); err != nil {
		return fmt.Errorf("parse stack template: %w", err)
	}

	if old != nil {
		return keepStackResources(stack, old, template)
	}

	tables, err := stackDynamoDBStorage()
	if err != nil {
		return err
	}

	params, err := stackSSMStorage()
	if err != nil {
		return err
	}

	refs := map[string]string{"AWS::StackName": stack.StackName, "AWS::StackId": stack.StackID, "AWS::Region": "us-east-1", "AWS::AccountId": "123456789012", "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com"}
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
				name, err = createStackTable(ctx, tables, properties, stack.StackName+"-"+resource.PhysicalResourceID)
			} else {
				name, err = createStackParameter(ctx, params, properties, "CFN-"+resource.PhysicalResourceID)
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
			refs[resource.LogicalResourceID] = name
		}
	}

	return nil
}

// Physical names survive an update only when the definition did not change.
func keepStackResources(stack *Stack, old *Stack, template s3StackTemplate) error {
	var oldTemplate s3StackTemplate
	if err := json.Unmarshal([]byte(old.TemplateBody), &oldTemplate); err != nil {
		return fmt.Errorf("parse previous stack template: %w", err)
	}

	prior := make(map[string]StackResource)
	for _, resource := range old.Resources {
		if resource.ResourceType == tableResource || resource.ResourceType == parameterResource {
			prior[resource.LogicalResourceID] = resource
		}
	}

	for i := range stack.Resources {
		resource := &stack.Resources[i]
		if resource.ResourceType != tableResource && resource.ResourceType != parameterResource {
			continue
		}

		previous, ok := prior[resource.LogicalResourceID]
		if !ok || previous.ResourceType != resource.ResourceType {
			return fmt.Errorf("adding %s %s during update is not supported", resource.ResourceType, resource.LogicalResourceID)
		}

		before, _ := json.Marshal(oldTemplate.Resources[resource.LogicalResourceID].Properties)
		after, _ := json.Marshal(template.Resources[resource.LogicalResourceID].Properties)

		if string(before) != string(after) {
			return fmt.Errorf("changing %s %s during update is not supported", resource.ResourceType, resource.LogicalResourceID)
		}

		resource.PhysicalResourceID = previous.PhysicalResourceID
		delete(prior, resource.LogicalResourceID)
	}

	for logicalID, resource := range prior {
		return fmt.Errorf("removing %s %s during update is not supported", resource.ResourceType, logicalID)
	}

	return nil
}

// The CFN property names used here match the DynamoDB API JSON, so a JSON round trip is the mapping.
func createStackTable(ctx context.Context, tables dynamodb.Storage, properties map[string]any, generatedName string) (string, error) {
	raw, err := json.Marshal(properties)
	if err != nil {
		return "", fmt.Errorf("marshal table properties: %w", err)
	}

	var req dynamodb.CreateTableRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return "", fmt.Errorf("map table properties: %w", err)
	}

	// CloudFormation generates a name when TableName is omitted.
	if req.TableName == "" {
		req.TableName = generatedName
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
func createStackParameter(ctx context.Context, params ssm.Storage, properties map[string]any, generatedName string) (string, error) {
	raw, err := json.Marshal(properties)
	if err != nil {
		return "", fmt.Errorf("marshal parameter properties: %w", err)
	}

	var req ssm.PutParameterRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return "", fmt.Errorf("map parameter properties: %w", err)
	}

	if req.Value == "" || req.Type == "" {
		return "", errors.New("SSM parameter requires Type and Value")
	}

	// CloudFormation generates a name when Name is omitted.
	if req.Name == "" {
		req.Name = generatedName
	}

	// An existing parameter is a conflict, as on real CloudFormation, never an overwrite.
	if _, err := params.PutParameter(ctx, &req); err != nil {
		return "", err
	}

	return req.Name, nil
}

// Parameters are removed before tables, mirroring creation order; resources already gone are skipped.
func deleteStackResources(ctx context.Context, stack *Stack) error {
	for _, kind := range []string{parameterResource, tableResource} {
		for _, resource := range stack.Resources {
			if resource.ResourceType != kind {
				continue
			}

			if kind == parameterResource {
				params, err := stackSSMStorage()
				if err != nil {
					return err
				}

				err = params.DeleteParameter(ctx, resource.PhysicalResourceID)

				var paramErr *ssm.ParameterError
				if err != nil && (!errors.As(err, &paramErr) || paramErr.Type != ssm.ErrParameterNotFound) {
					return err
				}

				continue
			}

			tables, err := stackDynamoDBStorage()
			if err != nil {
				return err
			}

			_, err = tables.DeleteTable(ctx, resource.PhysicalResourceID)

			var tableErr *dynamodb.TableError
			if err != nil && (!errors.As(err, &tableErr) || tableErr.Code != "ResourceNotFoundException") {
				return err
			}
		}
	}

	return nil
}
