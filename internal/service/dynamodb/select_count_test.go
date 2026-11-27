package dynamodb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// Select=COUNT was ignored, so a count Query or Scan returned every full row.
// DynamoDB answers COUNT with counts and no Items field; asserted on the raw
// body because the SDK masks an extra field.
func TestSelectCountReturnsNoItems(t *testing.T) {
	t.Parallel()

	store := NewMemoryStorage("http://localhost:4566")
	svc := New(store)

	if _, err := store.CreateTable(t.Context(), &CreateTableRequest{
		TableName:            "count-table",
		KeySchema:            []KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "sk", KeyType: "RANGE"}},
		AttributeDefinitions: []AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}, {AttributeName: "sk", AttributeType: "S"}},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for i := range 3 {
		if _, err := store.PutItem(t.Context(), "count-table", Item{
			"pk":   {S: ptr("p")},
			"sk":   {S: ptr(strconv.Itoa(i))},
			"body": {S: ptr("payload")},
		}, false, ConditionInput{}); err != nil {
			t.Fatalf("PutItem: %v", err)
		}
	}

	for target, body := range map[string]string{
		"DynamoDB_20120810.Query": `{"TableName":"count-table","Select":"COUNT","KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"p"}}}`,
		"DynamoDB_20120810.Scan":  `{"TableName":"count-table","Select":"COUNT"}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("X-Amz-Target", target)

		w := httptest.NewRecorder()
		svc.DispatchAction(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("%s status: got %d, body=%s", target, w.Code, w.Body.String())
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
			t.Fatalf("%s decode: %v; body=%s", target, err, w.Body.String())
		}

		if _, ok := raw["Items"]; ok {
			t.Fatalf("%s Select=COUNT must omit Items, body=%s", target, w.Body.String())
		}

		if got := string(raw["Count"]); got != "3" {
			t.Fatalf("%s Count: got %s, want 3", target, got)
		}
	}
}
