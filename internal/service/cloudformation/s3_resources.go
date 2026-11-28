package cloudformation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/s3"
)

const bucketResource = "AWS::S3::Bucket"
const policyResource = "AWS::S3::BucketPolicy"

// Resolve the registered store at execution time, after all service initializers run.
func stackS3Storage() (s3.Storage, error) {
	for _, svc := range service.Services() {
		if provider, ok := svc.(interface{ Storage() s3.Storage }); ok && svc.Name() == "s3" {
			return provider.Storage(), nil
		}
	}
	return nil, errors.New("S3 service is not registered")
}

type s3StackTemplate struct {
	Resources map[string]struct {
		Type       string
		Properties map[string]any
	}
	Parameters map[string]struct{ Default any }
}

// Policy references must resolve to actual physical bucket names, not logical IDs.
func stackValue(value any, refs map[string]string) (any, error) {
	switch v := value.(type) {
	case []any:
		out := make([]any, len(v))
		for i, element := range v {
			resolved, err := stackValue(element, refs)
			if err != nil {
				return nil, err
			}
			out[i] = resolved
		}
		return out, nil
	case map[string]any:
		if ref, ok := v["Ref"].(string); ok {
			resolved, exists := refs[ref]
			if !exists {
				return nil, fmt.Errorf("unresolved Ref %s", ref)
			}
			return resolved, nil
		}
		if attr, ok := v["Fn::GetAtt"]; ok {
			var resource, name string
			switch a := attr.(type) {
			case string:
				resource, name, _ = strings.Cut(a, ".")
			case []any:
				if len(a) == 2 {
					resource, _ = a[0].(string)
					name, _ = a[1].(string)
				}
			}
			if resolved, exists := refs[resource+"."+name]; exists {
				return resolved, nil
			}
			return nil, fmt.Errorf("unresolved GetAtt %s.%s", resource, name)
		}
		if sub, ok := v["Fn::Sub"]; ok {
			text, isString := sub.(string)
			local := make(map[string]string, len(refs))
			for key, value := range refs {
				local[key] = value
			}
			if !isString {
				args, ok := sub.([]any)
				if !ok || len(args) != 2 {
					return nil, errors.New("invalid Fn::Sub")
				}
				text, ok = args[0].(string)
				if !ok {
					return nil, errors.New("invalid Fn::Sub string")
				}
				variables, ok := args[1].(map[string]any)
				if !ok {
					return nil, errors.New("invalid Fn::Sub variables")
				}
				for key, value := range variables {
					resolved, err := stackValue(value, refs)
					if err != nil {
						return nil, err
					}
					local[key] = fmt.Sprint(resolved)
				}
			}
			var unresolved error
			out := regexp.MustCompile(`\$\{([^}]+)\}`).ReplaceAllStringFunc(text, func(token string) string {
				key := token[2 : len(token)-1]
				if strings.HasPrefix(key, "!") {
					return "${" + key[1:] + "}"
				}
				if resolved, ok := local[key]; ok {
					return resolved
				}
				unresolved = fmt.Errorf("unresolved Sub %s", key)
				return token
			})
			return out, unresolved
		}
		if join, ok := v["Fn::Join"].([]any); ok {
			if len(join) != 2 {
				return nil, errors.New("invalid Fn::Join")
			}
			separator, ok := join[0].(string)
			if !ok {
				return nil, errors.New("invalid join separator")
			}
			resolved, err := stackValue(join[1], refs)
			if err != nil {
				return nil, err
			}
			values, ok := resolved.([]any)
			if !ok {
				return nil, errors.New("invalid join values")
			}
			parts := make([]string, len(values))
			for i, value := range values {
				parts[i] = fmt.Sprint(value)
			}
			return strings.Join(parts, separator), nil
		}
		out := make(map[string]any, len(v))
		for key, element := range v {
			if strings.HasPrefix(key, "Fn::") {
				return nil, fmt.Errorf("unsupported intrinsic %s", key)
			}
			resolved, err := stackValue(element, refs)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	default:
		return value, nil
	}
}

// Create buckets before policies; undo each successful mutation if a later one fails.
func provisionStackS3(ctx context.Context, stack *Stack, old *Stack) (err error) {
	needed := false
	for _, resource := range stack.Resources {
		if resource.ResourceType == bucketResource || resource.ResourceType == policyResource {
			needed = true
		}
	}
	if old != nil {
		for _, resource := range old.Resources {
			if resource.ResourceType == bucketResource || resource.ResourceType == policyResource {
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
	store, err := stackS3Storage()
	if err != nil {
		return err
	}
	oldResources := make(map[string]StackResource)
	if old != nil {
		for _, resource := range old.Resources {
			if resource.ResourceType == bucketResource || resource.ResourceType == policyResource {
				oldResources[resource.LogicalResourceID] = resource
			}
		}
		// Updating policy contents must not lose physical identities or imply replacement.
		remaining := len(oldResources)
		for i := range stack.Resources {
			resource := &stack.Resources[i]
			if resource.ResourceType != bucketResource && resource.ResourceType != policyResource {
				continue
			}
			prior, ok := oldResources[resource.LogicalResourceID]
			if !ok || prior.ResourceType != resource.ResourceType {
				return errors.New("S3 resource replacement is not supported")
			}
			resource.PhysicalResourceID = prior.PhysicalResourceID
			remaining--
		}
		if remaining != 0 {
			return errors.New("S3 resource removal during update is not supported")
		}
	}
	refs := map[string]string{"AWS::StackName": stack.StackName, "AWS::StackId": stack.StackID, "AWS::Region": "us-east-1", "AWS::AccountId": "123456789012", "AWS::Partition": "aws"}
	for name, parameter := range template.Parameters {
		if parameter.Default != nil {
			refs[name] = fmt.Sprint(parameter.Default)
		}
	}
	for name, value := range stack.Parameters {
		refs[name] = value
	}
	var undo []func() error
	defer func() {
		if err != nil {
			for i := len(undo) - 1; i >= 0; i-- {
				err = errors.Join(err, undo[i]())
			}
		}
	}()
	for i := range stack.Resources {
		resource := &stack.Resources[i]
		if resource.ResourceType != bucketResource {
			continue
		}
		name := resource.PhysicalResourceID
		if configured := template.Resources[resource.LogicalResourceID].Properties["BucketName"]; configured != nil {
			resolved, resolveErr := stackValue(configured, refs)
			if resolveErr != nil {
				return resolveErr
			}
			var ok bool
			name, ok = resolved.(string)
			if !ok || name == "" {
				return errors.New("invalid BucketName")
			}
		}
		if prior, ok := oldResources[resource.LogicalResourceID]; ok {
			if prior.PhysicalResourceID != name {
				return errors.New("S3 bucket replacement is not supported")
			}
		} else {
			if err := store.CreateBucket(ctx, name); err != nil {
				return err
			}
			undo = append(undo, func() error { return store.DeleteBucket(ctx, name) })
		}
		resource.PhysicalResourceID = name
		refs[resource.LogicalResourceID] = name
		refs[resource.LogicalResourceID+".Arn"] = "arn:aws:s3:::" + name
	}
	for i := range stack.Resources {
		resource := &stack.Resources[i]
		if resource.ResourceType != policyResource {
			continue
		}
		properties := template.Resources[resource.LogicalResourceID].Properties
		resolved, resolveErr := stackValue(properties["Bucket"], refs)
		if resolveErr != nil {
			return resolveErr
		}
		bucket, ok := resolved.(string)
		if !ok || bucket == "" {
			return errors.New("BucketPolicy requires Bucket")
		}
		if prior, ok := oldResources[resource.LogicalResourceID]; ok && prior.PhysicalResourceID != bucket {
			return errors.New("BucketPolicy bucket replacement is not supported")
		}
		policy, resolveErr := stackValue(properties["PolicyDocument"], refs)
		if resolveErr != nil {
			return resolveErr
		}
		if _, ok := policy.(map[string]any); !ok {
			return errors.New("BucketPolicy requires PolicyDocument")
		}
		document, marshalErr := json.Marshal(policy)
		if marshalErr != nil {
			return marshalErr
		}
		previous, readErr := store.GetBucketPolicy(ctx, bucket)
		if readErr != nil {
			var bucketErr *s3.BucketError
			if !errors.As(readErr, &bucketErr) || bucketErr.Code != "NoSuchBucketPolicy" {
				return readErr
			}
		}
		if err := store.PutBucketPolicy(ctx, bucket, string(document)); err != nil {
			return err
		}
		undo = append(undo, func() error {
			if previous != "" {
				return store.PutBucketPolicy(ctx, bucket, previous)
			}
			return store.DeleteBucketPolicy(ctx, bucket)
		})
		resource.PhysicalResourceID = bucket
	}
	return nil
}

// Policies are removed first so references do not outlive stack-owned buckets.
func deleteStackS3(ctx context.Context, stack *Stack) error {
	var store s3.Storage
	for _, kind := range []string{policyResource, bucketResource} {
		for _, resource := range stack.Resources {
			if resource.ResourceType != kind {
				continue
			}
			if store == nil {
				var err error
				store, err = stackS3Storage()
				if err != nil {
					return err
				}
			}
			var err error
			if kind == policyResource {
				err = store.DeleteBucketPolicy(ctx, resource.PhysicalResourceID)
			} else {
				err = store.DeleteBucket(ctx, resource.PhysicalResourceID)
			}
			if err != nil {
				var bucketErr *s3.BucketError
				if !errors.As(err, &bucketErr) || bucketErr.Code != "NoSuchBucket" {
					return err
				}
			}
		}
	}
	return nil
}
