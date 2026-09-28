package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"

	"wallet/internal/app"
)

type ClientConfig struct {
	Region   string
	Endpoint string
}

func NewClient(ctx context.Context, cfg ClientConfig) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

type QueueNames struct {
	Wager  string
	DLQ    string
	Events string
}

type Queues struct {
	Wager  string
	DLQ    string
	Events string
}

func ResolveQueues(ctx context.Context, c *sqs.Client, n QueueNames) (Queues, error) {
	var q Queues
	for _, p := range []struct {
		name string
		url  *string
	}{{n.Wager, &q.Wager}, {n.DLQ, &q.DLQ}, {n.Events, &q.Events}} {
		out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(p.name)})
		if err != nil {
			return Queues{}, fmt.Errorf("resolve queue %s: %w", p.name, err)
		}
		*p.url = aws.ToString(out.QueueUrl)
	}
	return q, nil
}

type ProvisionConfig struct {
	Names       QueueNames
	Visibility  int32
	MaxReceives int32
	Principals  *Principals
}

func Provision(ctx context.Context, c *sqs.Client, cfg ProvisionConfig) (Queues, error) {
	var q Queues
	var err error
	fifo := map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "false",
		"MessageRetentionPeriod":    "1209600",
	}
	if q.DLQ, err = ensureQueue(ctx, c, cfg.Names.DLQ, fifo); err != nil {
		return q, err
	}
	attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.DLQ), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return q, err
	}
	redrive, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": attrs.Attributes[string(types.QueueAttributeNameQueueArn)],
		"maxReceiveCount":     strconv.Itoa(int(cfg.MaxReceives)),
	})
	input := map[string]string{
		"VisibilityTimeout": strconv.Itoa(int(cfg.Visibility)),
		"RedrivePolicy":     string(redrive),
	}
	for k, v := range fifo {
		input[k] = v
	}
	if q.Wager, err = ensureQueue(ctx, c, cfg.Names.Wager, input); err != nil {
		return q, err
	}
	if q.Events, err = ensureQueue(ctx, c, cfg.Names.Events, fifo); err != nil {
		return q, err
	}
	if cfg.Principals != nil {
		err = applyPolicies(ctx, c, q, *cfg.Principals)
	}
	return q, err
}

func ensureQueue(ctx context.Context, c *sqs.Client, name string, attrs map[string]string) (string, error) {
	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err == nil {
		return aws.ToString(out.QueueUrl), nil
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "QueueAlreadyExists" {
		return "", fmt.Errorf("create queue %s: %w", name, err)
	}
	u, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", err
	}
	settable := map[string]string{}
	for k, v := range attrs {
		if k != "FifoQueue" {
			settable[k] = v
		}
	}
	_, err = c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: u.QueueUrl, Attributes: settable})
	return aws.ToString(u.QueueUrl), err
}

type EventPublisher struct {
	client   *sqs.Client
	queueURL string
}

func NewEventPublisher(c *sqs.Client, q Queues) *EventPublisher {
	return &EventPublisher{client: c, queueURL: q.Events}
}

func (p *EventPublisher) PublishBatch(ctx context.Context, events []app.PendingEvent) (map[uuid.UUID]error, error) {
	entries := make([]types.SendMessageBatchRequestEntry, len(events))
	for i, e := range events {
		entries[i] = types.SendMessageBatchRequestEntry{
			Id:                     aws.String(strconv.Itoa(i)),
			MessageBody:            aws.String(e.Payload),
			MessageGroupId:         aws.String(e.PartitionKey),
			MessageDeduplicationId: aws.String(e.ID.String()),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"eventType": {DataType: aws.String("String"), StringValue: aws.String(e.EventType)},
				"eventId":   {DataType: aws.String("String"), StringValue: aws.String(e.ID.String())},
			},
		}
	}
	out, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(p.queueURL), Entries: entries})
	if err != nil {
		return nil, err
	}
	failed := map[uuid.UUID]error{}
	for _, f := range out.Failed {
		i, _ := strconv.Atoi(aws.ToString(f.Id))
		if i >= 0 && i < len(events) {
			failed[events[i].ID] = fmt.Errorf("%s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}
	return failed, nil
}

func Ping(ctx context.Context, c *sqs.Client, queueURL string) error {
	_, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	return err
}
