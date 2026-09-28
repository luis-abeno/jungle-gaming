package messaging

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestQueuePolicies(t *testing.T) {
	p := QueuePolicies("arn:wager", "arn:dlq", "arn:events", Principals{
		Producers: []string{"arn:producer"}, Service: "arn:service",
		EventReaders: []string{"arn:reader"}, Operators: []string{"arn:ops"},
	})
	type doc struct {
		Statement []struct {
			Principal map[string][]string
			Action    []string
			Resource  string
		}
	}
	allowed := func(queue, who, action string) bool {
		var d doc
		if err := json.Unmarshal([]byte(p[queue]), &d); err != nil {
			t.Fatal(err)
		}
		for _, s := range d.Statement {
			if s.Resource == queue && slices.Contains(s.Principal["AWS"], who) && slices.Contains(s.Action, action) {
				return true
			}
		}
		return false
	}
	checks := []struct {
		queue, who, action string
		want               bool
	}{
		{"arn:wager", "arn:producer", "sqs:SendMessage", true},
		{"arn:wager", "arn:producer", "sqs:ReceiveMessage", false},
		{"arn:wager", "arn:service", "sqs:ReceiveMessage", true},
		{"arn:wager", "arn:service", "sqs:SendMessage", false},
		{"arn:dlq", "arn:service", "sqs:SendMessage", true},
		{"arn:dlq", "arn:ops", "sqs:ReceiveMessage", true},
		{"arn:events", "arn:service", "sqs:SendMessage", true},
		{"arn:events", "arn:reader", "sqs:ReceiveMessage", true},
		{"arn:events", "arn:reader", "sqs:SendMessage", false},
		{"arn:events", "arn:producer", "sqs:SendMessage", false},
	}
	for _, c := range checks {
		if got := allowed(c.queue, c.who, c.action); got != c.want {
			t.Errorf("%s %s %s: got %v", c.queue, c.who, c.action, got)
		}
	}
}
