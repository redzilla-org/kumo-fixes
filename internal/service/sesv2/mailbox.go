package sesv2

import (
	"context"
	"fmt"
	"slices"

	"github.com/sivchari/kumo/internal/service/ses"
)

// Mailbox projects the existing v2 records into the common mailbox without
// sending a second email or allocating a different message identifier.
func (s *Service) Mailbox(ctx context.Context, address string) ([]*ses.SentEmail, error) {
	emails, err := s.storage.GetSentEmails(ctx)
	if err != nil {
		return nil, fmt.Errorf("read SESv2 mailbox: %w", err)
	}

	var result []*ses.SentEmail

	for _, email := range emails {
		var destinations []string
		if email.Destination != nil {
			destinations = append(destinations, email.Destination.ToAddresses...)
			destinations = append(destinations, email.Destination.CcAddresses...)
			destinations = append(destinations, email.Destination.BccAddresses...)
		}

		if email.FromEmailAddress != address && !slices.Contains(destinations, address) {
			continue
		}

		result = append(result, &ses.SentEmail{
			MessageID: email.MessageID, Source: email.FromEmailAddress,
			Subject: email.Subject, Body: email.Body, HTMLBody: email.HTMLBody,
			RawData: string(email.RawData), Destination: destinations, SentAt: email.SentAt,
		})
	}

	return result, nil
}
