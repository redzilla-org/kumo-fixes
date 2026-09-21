package dynamodb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const queryAction = "Query"

// Both accepted table identifiers must reach the same item through the HTTP API.
func TestReadItemByTableARNAndName(t *testing.T) {
	t.Parallel()

	store := NewMemoryStorage("http://localhost:4566")

	table, err := store.CreateTable(t.Context(), &CreateTableRequest{
		TableName:            "arn-read",
		KeySchema:            []KeySchemaElement{{AttributeName: "pk", KeyType: keyTypeHash}},
		AttributeDefinitions: []AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.PutItem(t.Context(), "arn-read", Item{"pk": {S: ptr("row")}}, false, ConditionInput{}); err != nil {
		t.Fatal(err)
	}

	svc := New(store)

	for _, identifier := range []string{"arn-read", table.TableARN} {
		for _, action := range []string{"GetItem", queryAction} {
			t.Run(action+"/"+identifier, func(t *testing.T) {
				body := fmt.Sprintf(`{"TableName":%q,"Key":{"pk":{"S":"row"}}}`, identifier)
				if action == queryAction {
					body = fmt.Sprintf(`{"TableName":%q,"KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{":pk":{"S":"row"}}}`, identifier)
				}

				r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
				r.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)

				w := httptest.NewRecorder()
				svc.DispatchAction(w, r)

				if w.Code != http.StatusOK {
					t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
				}

				assertTableReadItem(t, action, w.Body.Bytes())
			})
		}
	}
}

// Assert the API response rather than just successful dispatch.
func assertTableReadItem(t *testing.T, action string, body []byte) {
	t.Helper()

	var result struct {
		Item  Item
		Items []Item
	}

	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}

	item := result.Item

	if action == queryAction {
		if len(result.Items) != 1 {
			t.Fatalf("wanted one item, got %s", body)
		}

		item = result.Items[0]
	}

	if item["pk"].S == nil || *item["pk"].S != "row" {
		t.Fatalf("wrong item: %s", body)
	}
}
