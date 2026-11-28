package sfn

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// resolveIntrinsic implements States.MathAdd, States.StringToJson and
// States.Format, including nested intrinsic arguments. Grammar/escaping follows the AWS JSONPath intrinsic contract, not JSONata.
func resolveIntrinsic(expression string, input, contextData any, depth int) (any, error) {
	if depth > 10 {
		return nil, fmt.Errorf("intrinsic nesting exceeds 10")
	}
	open := strings.IndexByte(expression, '(')
	if open < 0 || !strings.HasSuffix(expression, ")") {
		return nil, fmt.Errorf("invalid intrinsic %q", expression)
	}
	name := strings.TrimSpace(expression[:open])
	arguments, err := intrinsicArguments(expression[open+1 : len(expression)-1])
	if err != nil {
		return nil, err
	}
	values := make([]any, len(arguments))
	for i, argument := range arguments {
		values[i], err = intrinsicArgument(argument, input, contextData, depth, name == "States.Format" && i == 0)
		if err != nil {
			return nil, err
		}
	}
	switch name {
	case "States.MathAdd":
		if len(values) != 2 {
			return nil, fmt.Errorf("States.MathAdd requires two numbers")
		}
		left, leftOK := values[0].(float64)
		right, rightOK := values[1].(float64)
		left, right = math.Round(left), math.Round(right)
		if !leftOK || !rightOK || left < math.MinInt32 || left > math.MaxInt32 || right < math.MinInt32 || right > math.MaxInt32 || left+right < math.MinInt32 || left+right > math.MaxInt32 {
			return nil, fmt.Errorf("States.MathAdd requires signed 32-bit integers and sum")
		}
		return left + right, nil
	case "States.StringToJson":
		if len(values) != 1 {
			return nil, fmt.Errorf("States.StringToJson requires one string")
		}
		encoded, ok := values[0].(string)
		if !ok {
			return nil, fmt.Errorf("States.StringToJson requires a string")
		}
		var decoded any
		if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
			return nil, fmt.Errorf("States.StringToJson: %w", err)
		}
		return decoded, nil
	case "States.Format":
		if len(values) == 0 {
			return nil, fmt.Errorf("States.Format requires a template")
		}
		template, ok := values[0].(string)
		if !ok {
			return nil, fmt.Errorf("States.Format template must be a string")
		}
		return formatIntrinsic(template, values[1:])
	default:
		return nil, fmt.Errorf("unsupported intrinsic %q", name)
	}
}

// Split only top-level commas: strings and nested intrinsic arguments can
// themselves contain commas, quotes, parentheses and bracketed reference paths.
func intrinsicArguments(source string) ([]string, error) {
	if strings.TrimSpace(source) == "" {
		return nil, nil
	}
	var arguments []string
	start, nesting := 0, 0
	quoted, escaped := false, false
	for i, char := range source {
		if escaped {
			escaped = false
			continue
		}
		if char == '\\' {
			escaped = true
			continue
		}
		if char == '\'' {
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		switch char {
		case '(', '[':
			nesting++
		case ')', ']':
			nesting--
		case ',':
			if nesting == 0 {
				arguments = append(arguments, strings.TrimSpace(source[start:i]))
				start = i + 1
			}
		}
		if nesting < 0 {
			return nil, fmt.Errorf("unbalanced intrinsic arguments")
		}
	}
	if quoted || escaped || nesting != 0 {
		return nil, fmt.Errorf("unclosed intrinsic argument")
	}
	return append(arguments, strings.TrimSpace(source[start:])), nil
}

func intrinsicArgument(argument string, input, contextData any, depth int, formatTemplate bool) (any, error) {
	switch {
	case strings.HasPrefix(argument, "States."):
		return resolveIntrinsic(argument, input, contextData, depth+1)
	case strings.HasPrefix(argument, "$$"):
		return resolveAnyJSONPath(argument[1:], contextData)
	case strings.HasPrefix(argument, "$"):
		return resolveAnyJSONPath(argument, input)
	case strings.HasPrefix(argument, "'") && strings.HasSuffix(argument, "'") && len(argument) >= 2:
		literal := argument[1 : len(argument)-1]
		if formatTemplate {
			return literal, nil
		}
		return unescapeIntrinsic(literal)
	default:
		var value any
		if err := json.Unmarshal([]byte(argument), &value); err != nil {
			return nil, fmt.Errorf("invalid intrinsic argument %q", argument)
		}
		return value, nil
	}
}

// Backslash escapes are interpreted after tokenization, preserving literal
// braces in Format templates instead of mistaking them for placeholders.
func unescapeIntrinsic(source string) (string, error) {
	var result strings.Builder
	for i := 0; i < len(source); i++ {
		if source[i] == '\\' {
			i++
			if i == len(source) || !strings.ContainsRune("'{}\\", rune(source[i])) {
				return "", fmt.Errorf("invalid intrinsic escape")
			}
		}
		result.WriteByte(source[i])
	}
	return result.String(), nil
}

func formatIntrinsic(template string, values []any) (string, error) {
	var result strings.Builder
	argument := 0
	for i := 0; i < len(template); i++ {
		switch template[i] {
		case '\\':
			i++
			if i == len(template) || !strings.ContainsRune("'{}\\", rune(template[i])) {
				return "", fmt.Errorf("invalid States.Format escape")
			}
			result.WriteByte(template[i])
		case '{':
			if i+1 == len(template) || template[i+1] != '}' || argument == len(values) {
				return "", fmt.Errorf("States.Format placeholder/argument mismatch")
			}
			if text, ok := values[argument].(string); ok {
				result.WriteString(text)
			} else {
				encoded, err := json.Marshal(values[argument])
				if err != nil {
					return "", err
				}
				result.Write(encoded)
			}
			argument++
			i++
		case '}':
			return "", fmt.Errorf("unescaped States.Format closing brace")
		default:
			result.WriteByte(template[i])
		}
	}
	if argument != len(values) {
		return "", fmt.Errorf("States.Format placeholder/argument mismatch")
	}
	return result.String(), nil
}
