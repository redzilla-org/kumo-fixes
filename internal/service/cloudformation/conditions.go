package cloudformation

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type conditionEvaluator struct {
	parameters  map[string]string
	definitions map[string]any
	values      map[string]bool
	visiting    map[string]bool
}

// Conditions are resolved before resource creation; an unselected branch must
// neither create a resource nor leave an AWS::NoValue placeholder in its properties.
func evaluateStackTemplate(body string, supplied map[string]string) (string, map[string]string, error) {
	var template map[string]any
	if err := json.Unmarshal([]byte(body), &template); err != nil {
		return "", nil, fmt.Errorf("parse template: %w", err)
	}

	parameters := make(map[string]string)
	if defaults, ok := template["Parameters"].(map[string]any); ok {
		for name, definition := range defaults {
			if entry, ok := definition.(map[string]any); ok {
				if value, exists := entry["Default"]; exists {
					parameters[name] = fmt.Sprint(value)
				}
			}
		}
	}

	for name, value := range supplied {
		parameters[name] = value
	}

	conditions, _ := template["Conditions"].(map[string]any)
	evaluator := &conditionEvaluator{parameters: parameters, definitions: conditions, values: make(map[string]bool), visiting: make(map[string]bool)}
	resources, _ := template["Resources"].(map[string]any)
	for name, definition := range resources {
		resource, ok := definition.(map[string]any)
		if !ok {
			return "", nil, fmt.Errorf("invalid resource %s", name)
		}

		if condition, exists := resource["Condition"]; exists {
			conditionName, ok := condition.(string)
			if !ok {
				return "", nil, fmt.Errorf("invalid condition on %s", name)
			}

			enabled, err := evaluator.named(conditionName)
			if err != nil {
				return "", nil, err
			}

			if !enabled {
				delete(resources, name)
				continue
			}
		}

		resolved, omit, err := evaluator.resolve(resource["Properties"])
		if err != nil {
			return "", nil, err
		}

		if omit {
			delete(resource, "Properties")
		} else if resolved != nil {
			resource["Properties"] = resolved
		}
	}

	encoded, err := json.Marshal(template)
	if err != nil {
		return "", nil, fmt.Errorf("encode evaluated template: %w", err)
	}

	return string(encoded), parameters, nil
}

func (e *conditionEvaluator) named(name string) (bool, error) {
	if value, ok := e.values[name]; ok {
		return value, nil
	}

	definition, ok := e.definitions[name]
	if !ok || e.visiting[name] {
		return false, fmt.Errorf("unresolved or cyclic condition %s", name)
	}

	e.visiting[name] = true
	value, err := e.condition(definition)
	delete(e.visiting, name)
	if err != nil {
		return false, err
	}

	e.values[name] = value

	return value, nil
}

func (e *conditionEvaluator) condition(value any) (bool, error) {
	definition, ok := value.(map[string]any)
	if !ok || len(definition) != 1 {
		return false, fmt.Errorf("invalid condition expression")
	}

	if name, ok := definition["Condition"].(string); ok {
		return e.named(name)
	}

	for operator, raw := range definition {
		arguments, ok := raw.([]any)
		if !ok {
			return false, fmt.Errorf("invalid %s arguments", operator)
		}

		if operator == "Fn::Equals" {
			if len(arguments) != 2 {
				return false, fmt.Errorf("Fn::Equals requires two arguments")
			}

			left, _, err := e.resolve(arguments[0])
			if err != nil {
				return false, err
			}

			right, _, err := e.resolve(arguments[1])
			if err != nil {
				return false, err
			}

			return reflect.DeepEqual(left, right), nil
		}

		if operator != "Fn::Not" && operator != "Fn::And" && operator != "Fn::Or" {
			return false, fmt.Errorf("unsupported condition %s", operator)
		}

		if (operator == "Fn::Not" && len(arguments) != 1) || (operator != "Fn::Not" && (len(arguments) < 2 || len(arguments) > 10)) {
			return false, fmt.Errorf("invalid %s argument count", operator)
		}

		result := operator == "Fn::And"
		for _, argument := range arguments {
			part, err := e.condition(argument)
			if err != nil {
				return false, err
			}

			switch operator {
			case "Fn::Not":
				result = !part
			case "Fn::And":
				result = result && part
			case "Fn::Or":
				result = result || part
			}
		}

		return result, nil
	}

	return false, fmt.Errorf("empty condition")
}

// The separate omit result removes both map properties and list entries.
func (e *conditionEvaluator) resolve(value any) (any, bool, error) {
	switch v := value.(type) {
	case map[string]any:
		if reference, ok := v["Ref"].(string); ok && len(v) == 1 {
			if reference == "AWS::NoValue" {
				return nil, true, nil
			}

			if parameter, ok := e.parameters[reference]; ok {
				return parameter, false, nil
			}

			// Resource and pseudo-parameter references belong to resource materialization.
			return v, false, nil
		}

		if raw, exists := v["Fn::If"]; exists {
			arguments, ok := raw.([]any)
			if !ok || len(arguments) != 3 {
				return nil, false, fmt.Errorf("Fn::If requires three arguments")
			}

			name, ok := arguments[0].(string)
			if !ok {
				return nil, false, fmt.Errorf("Fn::If requires a condition name")
			}

			enabled, err := e.named(name)
			if err != nil {
				return nil, false, err
			}

			if enabled {
				return e.resolve(arguments[1])
			}

			return e.resolve(arguments[2])
		}

		out := make(map[string]any, len(v))
		for key, entry := range v {
			resolved, omit, err := e.resolve(entry)
			if err != nil {
				return nil, false, err
			}

			if !omit {
				out[key] = resolved
			}
		}

		return out, false, nil
	case []any:
		out := make([]any, 0, len(v))
		for _, entry := range v {
			resolved, omit, err := e.resolve(entry)
			if err != nil {
				return nil, false, err
			}

			if !omit {
				out = append(out, resolved)
			}
		}

		return out, false, nil
	default:
		return value, false, nil
	}
}
