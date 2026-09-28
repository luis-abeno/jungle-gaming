package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"wallet/internal/app"
	"wallet/internal/domain"
	"wallet/internal/observability"
	"wallet/internal/observability/logctx"
)

const MessageType = "WagerTransactionRequested"

type ConsumerConfig struct {
	Name              string
	Workers           int
	WaitSeconds       int32
	VisibilityTimeout int32
	HandleTimeout     time.Duration
	AllowedSenders    map[string][]string
}

type MessageHandler interface {
	HandleMessage(ctx context.Context, msg app.Message) (app.MessageOutcome, error)
}

type Consumer struct {
	client  *sqs.Client
	queues  Queues
	handler MessageHandler
	cfg     ConsumerConfig
	log     *slog.Logger
	metrics *observability.Metrics
	faults  app.FaultHook

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewConsumer(client *sqs.Client, queues Queues, handler MessageHandler, cfg ConsumerConfig,
	log *slog.Logger, metrics *observability.Metrics, faults app.FaultHook) *Consumer {
	if faults == nil {
		faults = app.NoFaults
	}
	return &Consumer{client: client, queues: queues, handler: handler, cfg: cfg,
		log: log.With("consumer", cfg.Name), metrics: metrics, faults: faults}
}

func (c *Consumer) Start(context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	for i := 0; i < c.cfg.Workers; i++ {
		c.wg.Add(1)
		go c.poll(ctx)
	}
	c.log.Info("consumer started", "workers", c.cfg.Workers)
	return nil
}

func (c *Consumer) Stop(ctx context.Context) error {
	if c.cancel == nil {
		return nil
	}
	c.cancel()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
		c.log.Info("consumer stopped")
		return nil
	case <-ctx.Done():
		return errors.New("consumer did not drain in time")
	}
}

func (c *Consumer) poll(ctx context.Context) {
	defer c.wg.Done()
	failures := 0
	for ctx.Err() == nil {
		rctx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.WaitSeconds+10)*time.Second)
		out, err := c.client.ReceiveMessage(rctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.queues.Wager),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             c.cfg.WaitSeconds,
			VisibilityTimeout:           c.cfg.VisibilityTimeout,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			wait := min(time.Second<<min(failures, 5), 30*time.Second)
			c.log.Warn("receive failed", "error", err, "retryIn", wait.String())
			sleep(ctx, wait)
			continue
		}
		failures = 0
		for i, m := range out.Messages {
			if ctx.Err() != nil {
				c.release(out.Messages[i:])
				return
			}
			c.handle(ctx, m)
		}
	}
}

func (c *Consumer) handle(parent context.Context, m types.Message) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.cfg.HandleTimeout)
	defer cancel()
	ctx = logctx.With(ctx, "sqsMessageId", aws.ToString(m.MessageId))

	msg, reason, err := c.decode(m)
	if err != nil {
		c.log.WarnContext(ctx, "invalid message", "reason", reason, "error", err)
		c.deadLetter(ctx, m, reason)
		return
	}
	req := msg.Command.Request
	ctx = logctx.With(ctx, "messageId", msg.MessageID, "correlationId", msg.Command.CorrelationID,
		"providerId", req.ProviderID, "walletId", req.WalletID.String())

	out, err := c.handler.HandleMessage(ctx, msg)
	switch {
	case err == nil:
		c.faults("consumer.after_commit")
		outcome := "processed"
		if out.Duplicate {
			outcome = "duplicate"
			c.log.InfoContext(ctx, "duplicate message acknowledged")
		} else if t := out.Transaction; t != nil {
			c.log.InfoContext(logctx.With(ctx, "transactionId", t.ID().String()), "message handled",
				"status", t.Status(), "failureCode", t.FailureCode(), "idempotentReplay", out.Replay)
		}
		c.metrics.Messages.WithLabelValues(outcome).Inc()
		c.delete(ctx, m)
	case app.Transient(err):
		c.metrics.Messages.WithLabelValues("retry").Inc()
		c.metrics.Retry("sqs")
		c.retryLater(ctx, m, err)
	default:
		c.log.WarnContext(ctx, "message rejected permanently", "error", err)
		c.deadLetter(ctx, m, failureReason(err))
	}
}

func (c *Consumer) delete(ctx context.Context, m types.Message) {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(c.queues.Wager), ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		c.log.WarnContext(ctx, "delete failed after commit", "error", err)
	}
}

func (c *Consumer) retryLater(ctx context.Context, m types.Message, cause error) {
	receives, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	delay := min(int32(1)<<min(receives, 8), c.cfg.VisibilityTimeout)
	c.log.WarnContext(ctx, "transient failure, message will be retried",
		"error", cause, "receiveCount", receives, "retryInSeconds", delay)
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queues.Wager), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: delay,
	})
	if err != nil {
		c.log.WarnContext(ctx, "could not change visibility", "error", err)
	}
}

func (c *Consumer) deadLetter(ctx context.Context, m types.Message, reason string) {
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.queues.DLQ),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureReason":   {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"sourceMessageId": {DataType: aws.String("String"), StringValue: m.MessageId},
		},
	})
	if err != nil {
		c.log.ErrorContext(ctx, "could not move message to DLQ", "error", err)
		return
	}
	c.metrics.DeadLettered.WithLabelValues(reason).Inc()
	c.metrics.Messages.WithLabelValues("dead_lettered").Inc()
	c.delete(ctx, m)
}

func (c *Consumer) release(msgs []types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, m := range msgs {
		_, _ = c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(c.queues.Wager), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0,
		})
	}
}

type envelope struct {
	MessageID  string          `json:"messageId"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurredAt"`
	Data       json.RawMessage `json:"data"`
}

type wagerData struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	IdempotencyKey                 string    `json:"idempotencyKey"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          moneyData `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

type moneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func strict(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data")
	}
	return nil
}

func (c *Consumer) decode(m types.Message) (app.Message, string, error) {
	sender := m.Attributes[string(types.MessageSystemAttributeNameSenderId)]
	allowed, ok := c.cfg.AllowedSenders[sender]
	if !ok {
		allowed, ok = c.cfg.AllowedSenders["*"]
	}
	if !ok {
		return app.Message{}, "UNAUTHORIZED_SENDER", fmt.Errorf("sender %q is not allowed", sender)
	}

	var env envelope
	if err := strict([]byte(aws.ToString(m.Body)), &env); err != nil {
		return app.Message{}, "MALFORMED_MESSAGE", err
	}
	if env.MessageID == "" || len(env.MessageID) > 128 {
		return app.Message{}, "MALFORMED_MESSAGE", errors.New("messageId is required")
	}
	if env.Type != MessageType {
		return app.Message{}, "UNSUPPORTED_TYPE", fmt.Errorf("type %q", env.Type)
	}
	if env.OccurredAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, env.OccurredAt); err != nil {
			return app.Message{}, "MALFORMED_MESSAGE", errors.New("occurredAt must be RFC 3339")
		}
	}
	var d wagerData
	if err := strict(env.Data, &d); err != nil {
		return app.Message{}, "MALFORMED_MESSAGE", err
	}
	req, err := domain.NewWagerRequest(domain.WagerInput{
		ProviderID:                     d.ProviderID,
		ExternalTransactionID:          d.ExternalTransactionID,
		IdempotencyKey:                 d.IdempotencyKey,
		PlayerID:                       d.PlayerID,
		WalletID:                       d.WalletID,
		RoundID:                        d.RoundID,
		GameID:                         d.GameID,
		Kind:                           d.Kind,
		Amount:                         d.Money.Amount,
		Currency:                       d.Money.Currency,
		ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
	})
	if err != nil {
		return app.Message{}, "VALIDATION_ERROR", err
	}
	if !slices.Contains(allowed, "*") && !slices.Contains(allowed, req.ProviderID) {
		return app.Message{}, "FORBIDDEN_PROVIDER", fmt.Errorf("sender %q may not send for %s", sender, req.ProviderID)
	}

	hash := domain.CanonicalHash(map[string]any{
		"type": env.Type,
		"data": map[string]any{
			"providerId":                     req.ProviderID,
			"externalTransactionId":          req.ExternalTransactionID,
			"idempotencyKey":                 req.IdempotencyKey,
			"playerId":                       req.PlayerID.String(),
			"walletId":                       req.WalletID.String(),
			"roundId":                        req.RoundID,
			"gameId":                         req.GameID,
			"kind":                           string(req.Kind),
			"money":                          map[string]string{"amount": req.Money.Amount(), "currency": string(req.Money.Currency())},
			"referenceExternalTransactionId": req.ReferenceExternalTransactionID,
		},
	})
	return app.Message{
		Consumer:  c.cfg.Name,
		MessageID: env.MessageID,
		Hash:      hash,
		Command: app.Command{
			Request:       req,
			CorrelationID: env.MessageID,
			CausationID:   env.MessageID,
			Source:        "sqs",
		},
	}, "", nil
}

func failureReason(err error) string {
	switch {
	case errors.Is(err, app.ErrInboxConflict):
		return "MESSAGE_ID_CONFLICT"
	case errors.Is(err, app.ErrIdempotencyConflict):
		return "IDEMPOTENCY_KEY_CONFLICT"
	case errors.Is(err, app.ErrExternalIDConflict):
		return "EXTERNAL_TRANSACTION_CONFLICT"
	case errors.Is(err, app.ErrWalletNotFound):
		return "WALLET_NOT_FOUND"
	case errors.Is(err, domain.ErrWalletPlayerMismatch):
		return "WALLET_PLAYER_MISMATCH"
	case errors.Is(err, domain.ErrWalletCurrencyMismatch):
		return "CURRENCY_MISMATCH"
	case errors.Is(err, domain.ErrInvalid):
		return "VALIDATION_ERROR"
	}
	return "PROCESSING_FAILED"
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
