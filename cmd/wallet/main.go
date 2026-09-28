package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/fx"

	"wallet/internal/config"
	"wallet/internal/messaging"
	"wallet/internal/postgres"
	"wallet/internal/wiring"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cmd := "serve"
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		cfg, err := config.Load()
		if err != nil {
			return err
		}
		fx.New(wiring.Options(cfg)).Run()
		return nil
	case "migrate":
		return migrate(args)
	case "provision":
		return provision()
	case "healthcheck":
		return healthcheck()
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func migrate(args []string) error {
	url := env("MIGRATION_DATABASE_URL", os.Getenv("DATABASE_URL"))
	if url == "" {
		return errors.New("MIGRATION_DATABASE_URL or DATABASE_URL is required")
	}
	if len(args) == 0 {
		return errors.New("usage: wallet migrate up|down [n|all]|version|force <version>")
	}
	steps := 1
	if len(args) > 1 {
		if args[1] == "all" {
			steps = -1
		} else {
			n, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid step count %q", args[1])
			}
			steps = n
		}
	}
	v, dirty, err := postgres.Migrate(url, args[0], steps)
	if err != nil {
		return err
	}
	fmt.Printf("schema version %d (dirty=%t)\n", v, dirty)
	return nil
}

func provision() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := messaging.NewClient(ctx, messaging.ClientConfig{
		Region: env("AWS_REGION", "us-east-1"), Endpoint: os.Getenv("AWS_ENDPOINT_URL"),
	})
	if err != nil {
		return err
	}
	visibility, _ := strconv.Atoi(env("SQS_VISIBILITY_TIMEOUT", "30"))
	receives, _ := strconv.Atoi(env("SQS_MAX_RECEIVES", "5"))
	names := messaging.QueueNames{
		Wager:  env("SQS_WAGER_QUEUE", "wager-transactions.fifo"),
		DLQ:    env("SQS_WAGER_DLQ", "wager-transactions-dlq.fifo"),
		Events: env("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
	}
	account := env("AWS_ACCOUNT_ID", "000000000000")
	arn := func(role string) string { return "arn:aws:iam::" + account + ":role/" + role }
	principals := &messaging.Principals{
		Producers:    list(env("SQS_PRODUCER_PRINCIPALS", arn("wager-gateway"))),
		Service:      env("SQS_SERVICE_PRINCIPAL", arn("wallet-service")),
		EventReaders: list(env("SQS_EVENT_READER_PRINCIPALS", arn("wallet-events-reader"))),
		Operators:    list(env("SQS_OPERATOR_PRINCIPALS", arn("wallet-operator"))),
	}
	var q messaging.Queues
	for attempt := 1; ; attempt++ {
		q, err = messaging.Provision(ctx, client, messaging.ProvisionConfig{
			Names: names, Visibility: int32(visibility), MaxReceives: int32(receives), Principals: principals,
		})
		if err == nil || attempt == 20 {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return err
	}
	fmt.Println("queues ready:", strings.Join([]string{q.Wager, q.DLQ, q.Events}, " "))
	return nil
}

func list(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func healthcheck() error {
	addr := env("HTTP_ADDR", ":8080")
	if strings.HasPrefix(addr, ":") {
		addr = "localhost" + addr
	}
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/health/ready")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ready returned %s", resp.Status)
	}
	return nil
}
