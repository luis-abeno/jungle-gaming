package messaging

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Principals struct {
	Producers    []string
	Service      string
	EventReaders []string
	Operators    []string
}

type statement struct {
	Sid       string              `json:"Sid"`
	Effect    string              `json:"Effect"`
	Principal map[string][]string `json:"Principal"`
	Action    []string            `json:"Action"`
	Resource  string              `json:"Resource"`
	Condition map[string]any      `json:"Condition,omitempty"`
}

func policy(stmts ...statement) string {
	b, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": stmts})
	return string(b)
}

func allow(sid string, who []string, arn string, actions ...string) statement {
	return statement{Sid: sid, Effect: "Allow", Principal: map[string][]string{"AWS": who}, Action: actions, Resource: arn}
}

func QueuePolicies(wagerARN, dlqARN, eventsARN string, p Principals) map[string]string {
	service := []string{p.Service}
	return map[string]string{
		wagerARN: policy(
			allow("ProducersSend", p.Producers, wagerARN, "sqs:SendMessage", "sqs:GetQueueUrl"),
			allow("ServiceConsumes", service, wagerARN, "sqs:ReceiveMessage", "sqs:DeleteMessage",
				"sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"),
		),
		dlqARN: policy(
			allow("ServiceDeadLetters", service, dlqARN, "sqs:SendMessage", "sqs:GetQueueUrl"),
			allow("OperatorsInspect", p.Operators, dlqARN, "sqs:ReceiveMessage", "sqs:DeleteMessage",
				"sqs:GetQueueAttributes", "sqs:StartMessageMoveTask"),
		),
		eventsARN: policy(
			allow("ServicePublishes", service, eventsARN, "sqs:SendMessage", "sqs:GetQueueUrl"),
			allow("ReadersConsume", p.EventReaders, eventsARN, "sqs:ReceiveMessage", "sqs:DeleteMessage",
				"sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"),
		),
	}
}

func applyPolicies(ctx context.Context, c *sqs.Client, q Queues, p Principals) error {
	arns := map[string]string{}
	for _, u := range []string{q.Wager, q.DLQ, q.Events} {
		out, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(u), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
		if err != nil {
			return err
		}
		arns[u] = out.Attributes[string(types.QueueAttributeNameQueueArn)]
	}
	policies := QueuePolicies(arns[q.Wager], arns[q.DLQ], arns[q.Events], p)
	for u, arn := range arns {
		_, err := c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl: aws.String(u), Attributes: map[string]string{"Policy": policies[arn]},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
