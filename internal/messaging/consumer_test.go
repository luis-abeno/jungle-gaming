package messaging

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const validBody = `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
"data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123",
"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37",
"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`

func msg(body, sender string) types.Message {
	return types.Message{Body: aws.String(body), Attributes: map[string]string{"SenderId": sender}}
}

func consumer(senders map[string][]string) *Consumer {
	return &Consumer{cfg: ConsumerConfig{Name: "c", AllowedSenders: senders}}
}

func TestDecodeValidMessage(t *testing.T) {
	c := consumer(map[string][]string{"acct": {"provider-a"}})
	m, reason, err := c.decode(msg(validBody, "acct"))
	if err != nil {
		t.Fatalf("%s %v", reason, err)
	}
	if m.MessageID != "msg-123" || m.Command.Request.IdempotencyKey != "provider-a:transaction-123" || m.Command.Source != "sqs" {
		t.Fatalf("%+v", m)
	}

	reordered := `{"type":"WagerTransactionRequested","messageId":"msg-123","data":{"money":{"currency":"BRL","amount":"25.00"},
"kind":"BET","gameId":"fortune-chimp","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37",
"playerId":"0192F28F-5DC0-7D58-BDB2-814AD6A0F4A1","idempotencyKey":"provider-a:transaction-123",
"externalTransactionId":"transaction-123","providerId":"provider-a"}}`
	m2, _, err := c.decode(msg(reordered, "acct"))
	if err != nil || m2.Hash != m.Hash {
		t.Fatalf("hash differs: %v", err)
	}
	changed, _, _ := c.decode(msg(strings.Replace(validBody, "25.00", "26.00", 1), "acct"))
	if changed.Hash == m.Hash {
		t.Fatal("content change must change the hash")
	}
}

func TestDecodeRejects(t *testing.T) {
	c := consumer(map[string][]string{"acct": {"provider-a"}})
	cases := map[string]struct {
		body, sender, reason string
	}{
		"unknown sender":    {validBody, "stranger", "UNAUTHORIZED_SENDER"},
		"other provider":    {strings.Replace(validBody, `"providerId":"provider-a"`, `"providerId":"provider-b"`, 1), "acct", "FORBIDDEN_PROVIDER"},
		"not json":          {"hello", "acct", "MALFORMED_MESSAGE"},
		"no message id":     {strings.Replace(validBody, `"msg-123"`, `""`, 1), "acct", "MALFORMED_MESSAGE"},
		"wrong type":        {strings.Replace(validBody, "WagerTransactionRequested", "Other", 1), "acct", "UNSUPPORTED_TYPE"},
		"unknown field":     {strings.Replace(validBody, `"kind":"BET"`, `"kind":"BET","extra":1`, 1), "acct", "MALFORMED_MESSAGE"},
		"numeric amount":    {strings.Replace(validBody, `"25.00"`, `25.00`, 1), "acct", "MALFORMED_MESSAGE"},
		"bad occurredAt":    {strings.Replace(validBody, "2026-09-08T12:00:00.000Z", "yesterday", 1), "acct", "MALFORMED_MESSAGE"},
		"opening":           {strings.Replace(validBody, `"kind":"BET"`, `"kind":"OPENING"`, 1), "acct", "VALIDATION_ERROR"},
		"missing key":       {strings.Replace(validBody, `"provider-a:transaction-123"`, `""`, 1), "acct", "VALIDATION_ERROR"},
		"trailing garbage":  {validBody + "{}", "acct", "MALFORMED_MESSAGE"},
		"negative amount":   {strings.Replace(validBody, `"25.00"`, `"-25.00"`, 1), "acct", "VALIDATION_ERROR"},
		"scientific amount": {strings.Replace(validBody, `"25.00"`, `"2.5e1"`, 1), "acct", "VALIDATION_ERROR"},
	}
	for name, tc := range cases {
		_, reason, err := c.decode(msg(tc.body, tc.sender))
		if err == nil || reason != tc.reason {
			t.Errorf("%s: got %q %v, want %s", name, reason, err, tc.reason)
		}
	}
}

func TestWildcardSender(t *testing.T) {
	c := consumer(map[string][]string{"*": {"*"}})
	if _, reason, err := c.decode(msg(validBody, "anyone")); err != nil {
		t.Fatalf("%s %v", reason, err)
	}
}
