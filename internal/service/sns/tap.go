package sns

import (
	"encoding/json"
	"net/http"
	"time"
)

// SNS message tap. A test that wants to prove its code published a
// notification has to read what was published to the topic, but on kumo such
// a topic often has no subscriber to deliver to, so the publish leaves no
// trace. Every successful Publish is therefore recorded per topic,
// independent of subscriptions, and served over a kumo-specific REST endpoint
// beside the SES mailbox endpoint (/_aws/ses).

// snsTapPath is the tap endpoint: GET lists a topic's recording, DELETE clears it.
const snsTapPath = "/_aws/sns/messages"

// snsTapTimestampLayout is RFC3339 with millisecond precision, the shape AWS
// uses for the Timestamp field of an SNS notification.
const snsTapTimestampLayout = "2006-01-02T15:04:05.000Z07:00"

// PublishedMessage is one recorded Publish as the tap serves it.
type PublishedMessage struct {
	MessageID         string                      `json:"MessageId"`
	TopicArn          string                      `json:"TopicArn"`
	Subject           string                      `json:"Subject"`
	Message           string                      `json:"Message"`
	Timestamp         string                      `json:"Timestamp"`
	MessageAttributes map[string]MessageAttribute `json:"MessageAttributes"`
}

// publishedMessagesResponse is the GET body; Messages is never null.
type publishedMessagesResponse struct {
	Messages []PublishedMessage `json:"messages"`
}

// clearedMessagesResponse is the DELETE body, naming how many were dropped.
type clearedMessagesResponse struct {
	Cleared int `json:"cleared"`
}

// recordPublish appends one successful Publish to its topic's recording.
// A separate mutex keeps the tap off the topic/subscription lock, which
// Publish has already released before delivery.
func (m *MemoryStorage) recordPublish(topicARN, messageID, subject, message string, attributes map[string]MessageAttribute) {
	// Copy the attributes so a caller mutating its map cannot rewrite history.
	attrs := make(map[string]MessageAttribute, len(attributes))
	for k, v := range attributes {
		attrs[k] = v
	}

	m.tapMu.Lock()
	defer m.tapMu.Unlock()

	if m.published == nil {
		m.published = make(map[string][]PublishedMessage)
	}

	m.published[topicARN] = append(m.published[topicARN], PublishedMessage{
		MessageID:         messageID,
		TopicArn:          topicARN,
		Subject:           subject,
		Message:           message,
		Timestamp:         time.Now().UTC().Format(snsTapTimestampLayout),
		MessageAttributes: attrs,
	})
}

// PublishedMessages returns a copy of the topic's recording, oldest first.
// An unknown or never-published topic yields an empty, non-nil slice.
func (m *MemoryStorage) PublishedMessages(topicARN string) []PublishedMessage {
	m.tapMu.Lock()
	defer m.tapMu.Unlock()

	recorded := m.published[topicARN]
	out := make([]PublishedMessage, len(recorded))
	copy(out, recorded)

	return out
}

// ClearPublishedMessages drops the topic's recording and returns its size.
func (m *MemoryStorage) ClearPublishedMessages(topicARN string) int {
	m.tapMu.Lock()
	defer m.tapMu.Unlock()

	n := len(m.published[topicARN])
	delete(m.published, topicARN)

	return n
}

// GetPublishedMessages serves GET /_aws/sns/messages?topicArn=<arn>.
func (s *Service) GetPublishedMessages(w http.ResponseWriter, r *http.Request) {
	topicARN := r.URL.Query().Get("topicArn")
	if topicARN == "" {
		writeTapJSON(w, http.StatusBadRequest, map[string]string{"error": "topicArn query parameter is required"})

		return
	}

	writeTapJSON(w, http.StatusOK, publishedMessagesResponse{Messages: s.storage.PublishedMessages(topicARN)})
}

// ClearPublishedMessages serves DELETE /_aws/sns/messages?topicArn=<arn>.
func (s *Service) ClearPublishedMessages(w http.ResponseWriter, r *http.Request) {
	topicARN := r.URL.Query().Get("topicArn")
	if topicARN == "" {
		writeTapJSON(w, http.StatusBadRequest, map[string]string{"error": "topicArn query parameter is required"})

		return
	}

	writeTapJSON(w, http.StatusOK, clearedMessagesResponse{Cleared: s.storage.ClearPublishedMessages(topicARN)})
}

// writeTapJSON writes the tap's plain JSON (not the SNS Query XML) response.
func writeTapJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
