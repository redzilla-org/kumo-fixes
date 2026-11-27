package dynamodb

import (
	"strconv"
	"strings"
)

type updatePathPart struct {
	name  string
	index int
	list  bool
}

// Split syntax before expanding aliases: a dot inside an attribute name is data.
func parseUpdatePath(raw string, names map[string]string) ([]updatePathPart, error) {
	raw = strings.TrimSpace(raw)
	var parts []updatePathPart
	for len(raw) > 0 {
		end := strings.IndexAny(raw, ".[ ")
		if end < 0 {
			end = len(raw)
		}
		if end == 0 {
			return nil, newValidationException("Invalid document path")
		}
		name := raw[:end]
		if strings.HasPrefix(name, "#") {
			var ok bool
			name, ok = names[name]
			if !ok {
				return nil, newValidationException("Undefined expression attribute name")
			}
		}
		parts = append(parts, updatePathPart{name: name})
		raw = raw[end:]
		for strings.HasPrefix(raw, "[") {
			end = strings.IndexByte(raw, ']')
			if end < 0 {
				return nil, newValidationException("Invalid list index")
			}
			index, err := strconv.Atoi(raw[1:end])
			if err != nil || index < 0 {
				return nil, newValidationException("Invalid list index")
			}
			parts = append(parts, updatePathPart{list: true, index: index})
			raw = raw[end+1:]
		}
		if raw == "" {
			break
		}
		if raw[0] != '.' || len(raw) == 1 {
			return nil, newValidationException("Invalid document path")
		}
		raw = raw[1:]
	}
	if len(parts) == 0 {
		return nil, newValidationException("Empty document path")
	}
	return parts, nil
}

func readUpdatePath(item Item, parts []updatePathPart) (AttributeValue, bool) {
	value, ok := item[parts[0].name]
	for _, part := range parts[1:] {
		if !ok {
			return AttributeValue{}, false
		}
		var next *AttributeValue
		if part.list {
			if part.index < len(value.L) {
				next = value.L[part.index]
			}
		} else {
			next = value.M[part.name]
		}
		ok = next != nil
		if ok {
			value = *next
		}
	}
	return value, ok
}

// Rebuild containers so an unsuccessful update never partially mutates stored data.
func writeUpdateChild(parent AttributeValue, parts []updatePathPart, value *AttributeValue) (AttributeValue, error) {
	part := parts[0]
	if part.list {
		if parent.L == nil {
			return parent, newValidationException("Invalid document path for update")
		}
		if len(parts) == 1 {
			if part.index >= len(parent.L) {
				if value != nil {
					parent.L = append(parent.L, value)
				}
				return parent, nil
			}
			if value == nil {
				parent.L = append(parent.L[:part.index], parent.L[part.index+1:]...)
			} else {
				parent.L[part.index] = value
			}
			return parent, nil
		}
		if part.index >= len(parent.L) || parent.L[part.index] == nil {
			return parent, newValidationException("Invalid document path for update")
		}
		child, err := writeUpdateChild(*parent.L[part.index], parts[1:], value)
		if err == nil {
			parent.L[part.index] = &child
		}
		return parent, err
	}
	if parent.M == nil {
		return parent, newValidationException("Invalid document path for update")
	}
	if len(parts) == 1 {
		if value == nil {
			delete(parent.M, part.name)
		} else {
			parent.M[part.name] = value
		}
		return parent, nil
	}
	child := parent.M[part.name]
	if child == nil {
		return parent, newValidationException("Invalid document path for update")
	}
	updated, err := writeUpdateChild(*child, parts[1:], value)
	if err == nil {
		parent.M[part.name] = &updated
	}
	return parent, err
}

func updateOperand(item Item, expr string, names map[string]string, values map[string]AttributeValue) (AttributeValue, bool) {
	expr = strings.TrimSpace(expr)
	if value, ok := values[expr]; ok {
		return value, true
	}
	if strings.HasPrefix(expr, "if_not_exists(") && strings.HasSuffix(expr, ")") {
		args := splitAssignments(expr[len("if_not_exists(") : len(expr)-1])
		if len(args) != 2 {
			return AttributeValue{}, false
		}
		if value, ok := updateOperand(item, args[0], names, values); ok {
			return value, true
		}
		return updateOperand(item, args[1], names, values)
	}
	for _, op := range []string{" + ", " - "} {
		if index := strings.LastIndex(expr, op); index >= 0 {
			left, lok := updateOperand(item, expr[:index], names, values)
			right, rok := updateOperand(item, expr[index+len(op):], names, values)
			if !lok || !rok {
				return AttributeValue{}, false
			}
			return evaluateSetArithmetic(nil, ":left"+op+":right", map[string]AttributeValue{":left": left, ":right": right})
		}
	}
	parts, err := parseUpdatePath(expr, names)
	if err != nil {
		return AttributeValue{}, false
	}
	return readUpdatePath(item, parts)
}

func (m *MemoryStorage) applyDocumentUpdate(item Item, expr string, names map[string]string, values map[string]AttributeValue) (Item, error) {
	result := m.copyItem(item)
	if result == nil {
		result = make(Item)
	}
	for _, clause := range parseUpdateClauses(expr) {
		for _, action := range splitAssignments(clause.body) {
			var raw, operand string
			if clause.action == updateActionSet {
				pieces := strings.SplitN(action, "=", 2)
				if len(pieces) != 2 {
					return nil, newValidationException("Invalid SET expression")
				}
				raw, operand = pieces[0], pieces[1]
			} else {
				fields := strings.Fields(action)
				if len(fields) == 0 {
					return nil, newValidationException("Invalid update expression")
				}
				raw = fields[0]
				if len(fields) == 2 {
					operand = fields[1]
				}
			}
			parts, err := parseUpdatePath(raw, names)
			if err != nil {
				return nil, err
			}
			var replacement *AttributeValue
			if clause.action != updateActionRem {
				value, ok := updateOperand(result, operand, names, values)
				if !ok {
					return nil, newValidationException("Update operand does not exist")
				}
				if clause.action == updateActionAdd || clause.action == updateActionDel {
					leaf := make(Item)
					if old, exists := readUpdatePath(result, parts); exists {
						leaf["value"] = old
					}
					args := map[string]AttributeValue{":value": value}
					if clause.action == updateActionAdd {
						applyAddClause(leaf, "value :value", args)
					} else {
						applyDeleteClause(leaf, "value :value", args)
					}
					value, ok = leaf["value"]
				}
				if ok {
					replacement = &value
				}
			}
			if len(parts) == 1 {
				if replacement == nil {
					delete(result, parts[0].name)
				} else {
					result[parts[0].name] = *replacement
				}
			} else {
				parent, exists := result[parts[0].name]
				if !exists {
					return nil, newValidationException("Invalid document path for update")
				}
				parent, err = writeUpdateChild(parent, parts[1:], replacement)
				if err != nil {
					return nil, err
				}
				result[parts[0].name] = parent
			}
		}
	}
	return result, nil
}
