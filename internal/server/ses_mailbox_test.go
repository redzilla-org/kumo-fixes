package server

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkses "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/google/uuid"

	"github.com/sivchari/kumo/internal/service/ses"
	_ "github.com/sivchari/kumo/internal/service/sesv2"
)

// Use the SDK's native route and the common mailbox, not the v2 storage projection directly.
func TestSESAPIsShareRecipientMailbox(t *testing.T) {
	t.Parallel()

	host := httptest.NewServer(New(DefaultConfig()).Handler())
	t.Cleanup(host.Close)

	prefix := uuid.NewString()
	sender := prefix + "-sender@example.test"
	recipients := []string{prefix + "-to@example.test", prefix + "-cc@example.test", prefix + "-bcc@example.test"}
	client := sdkses.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}, func(o *sdkses.Options) {
		o.BaseEndpoint = aws.String(host.URL)
	})

	sent, err := client.SendEmail(t.Context(), &sdkses.SendEmailInput{
		FromEmailAddress: aws.String(sender),
		Destination:      &types.Destination{ToAddresses: recipients[:1], CcAddresses: recipients[1:2], BccAddresses: recipients[2:]},
		Content: &types.EmailContent{Simple: &types.Message{
			Subject: &types.Content{Data: aws.String("v2 subject")},
			Body:    &types.Body{Text: &types.Content{Data: aws.String("v2 body")}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if aws.ToString(sent.MessageId) == "" {
		t.Fatal("SDK returned empty message ID")
	}

	v1ID := sendMailboxV1(t, host, sender, recipients)
	want := map[string]string{v1ID: "v1 subject", aws.ToString(sent.MessageId): "v2 subject"}

	for _, address := range append(recipients, sender) {
		assertSharedMailbox(t, host, address, want)
	}
}

// Keep v1 in the same integration to protect existing sender lookup and message identity.
func sendMailboxV1(t *testing.T, host *httptest.Server, sender string, recipients []string) string {
	t.Helper()

	form := url.Values{
		"Action": {"SendEmail"}, "Version": {"2010-12-01"}, "Source": {sender},
		"Destination.ToAddresses.member.1":  {recipients[0]},
		"Destination.CcAddresses.member.1":  {recipients[1]},
		"Destination.BccAddresses.member.1": {recipients[2]},
		"Message.Subject.Data":              {"v1 subject"}, "Message.Body.Text.Data": {"v1 body"},
	}

	r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, host.URL, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}

	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("User-Agent", "aws-sdk-go-v2 api/ses#1.0.0")

	resp, err := host.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()

	var result ses.XMLSendEmailResponse
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK || result.SendEmailResult.MessageID == "" {
		t.Fatalf("v1 status=%d response=%+v", resp.StatusCode, result)
	}

	return result.SendEmailResult.MessageID
}

func assertSharedMailbox(t *testing.T, host *httptest.Server, address string, want map[string]string) {
	t.Helper()

	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, host.URL+"/_aws/ses?email="+url.QueryEscape(address), http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := host.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()

	var messages []ses.SentEmail
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK || len(messages) != 2 {
		t.Fatalf("mailbox %s: status=%d messages=%+v", address, resp.StatusCode, messages)
	}

	got := make(map[string]string)
	for i := range messages {
		got[messages[i].MessageID] = messages[i].Subject
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mailbox identity mismatch: got=%v want=%v", got, want)
	}
}
