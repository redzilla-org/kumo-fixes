package sns

import (
	"encoding/json"
	"strings"
	"testing"
)

// Instances configured for a non-default region used to create us-east-1
// topics, so clients using their own regional ARN could not subscribe or publish.
func TestSNSRegionSurvivesCreateSubscribePublish(t *testing.T) {
	cases := []struct {
		region   string
		name     string
		topicARN string
		queueARN string
	}{
		{"us-west-2", "west-events", "arn:aws:sns:us-west-2:000000000000:west-events", "arn:aws:sqs:us-west-2:000000000000:app-events"},
		{"us-east-2", "east-events", "arn:aws:sns:us-east-2:000000000000:east-events", "arn:aws:sqs:us-east-2:000000000000:app-events"},
		{"", "default-events", "arn:aws:sns:us-east-1:000000000000:default-events", "arn:aws:sqs:us-east-1:000000000000:app-events"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_DEFAULT_REGION", tc.region)
			publisher := &capturingPublisher{}
			store := NewMemoryStorage("http://localhost:4566")
			store.SetSQSPublisher(publisher)
			topic, err := store.CreateTopic(t.Context(), tc.name, nil, nil)
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			if topic.ARN != tc.topicARN {
				t.Fatalf("topic ARN = %q, want %q", topic.ARN, tc.topicARN)
			}

			// Use the client's configured ARN rather than propagating a wrong
			// CreateTopic result through the rest of the notification journey.
			sub, err := store.Subscribe(t.Context(), tc.topicARN, "sqs", tc.queueARN, nil)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			if sub.TopicARN != tc.topicARN || !strings.HasPrefix(sub.ARN, tc.topicARN+":") {
				t.Fatalf("subscription identity = %#v, want topic %q", sub, tc.topicARN)
			}
			messageID, err := store.Publish(t.Context(), tc.topicARN, "hello", "", "", "", nil)
			if err != nil || messageID == "" {
				t.Fatalf("Publish: message ID %q, error %v", messageID, err)
			}

			// Delivery must retain the regional identity and publish receipt,
			// proving the complete SNS-to-SQS storage path was reached.
			var notification struct {
				TopicARN  string `json:"TopicArn"`
				MessageID string `json:"MessageId"`
				Message   string `json:"Message"`
			}
			if err := json.Unmarshal([]byte(publisher.body), &notification); err != nil {
				t.Fatalf("notification envelope: %v", err)
			}
			if publisher.endpoint != tc.queueARN || notification.TopicARN != tc.topicARN || notification.MessageID != messageID || notification.Message != "hello" {
				t.Fatalf("delivery endpoint %q, notification %#v", publisher.endpoint, notification)
			}
		})
	}
}
