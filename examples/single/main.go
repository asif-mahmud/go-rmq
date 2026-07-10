package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	gormq "github.com/asif-mahmud/go-rmq"
)

func main() {
	gormq.Init(gormq.ConnectionOptions{
		URL:                        "amqp://admin:admin@localhost:5672",
		ReconnectInterval:          10 * time.Second,
		FailedMessageRetryInterval: 1 * time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gormq.GetClient().StartWithContext(ctx)

	dataChan := make(chan []byte)

	consumerID, err := gormq.GetClient().AddConsumerWithContext(ctx, gormq.ConsumerOption{
		Exchange:   "rmq-test-exchange",
		RoutingKey: "rmq-test-route",
		Queue:      "rmq-test:rmq-test-route:queue",
		ConsumerWithContext: func(c context.Context, d []byte) error {
			dataChan <- d
			slog.InfoContext(c, string(d))
			return nil
		},
		PrefetchCount: 10,
	})
	if err != nil {
		slog.Error("Failed to add consumer", "error", err)
		return
	}
	slog.Info("Consumer added successfully", "id", consumerID)

	time.Sleep(5 * time.Second)

	err = gormq.GetClient().
		PublishWithContext(ctx, gormq.NewMessage(
			"rmq-test-exchange",
			"rmq-test-route",
			`{"success":true}`,
		))
	if err != nil {
		slog.Error("Failed to publish", "error", err)
	}

	msg := <-dataChan
	slog.Info(fmt.Sprintf("Received: %s", string(msg)))

	// Close the specific consumer by its ID
	if err := gormq.GetClient().CloseConsumerWithContext(ctx, consumerID); err != nil {
		slog.Error("Failed to close consumer", "id", consumerID, "error", err)
	} else {
		slog.Info("Consumer closed successfully", "id", consumerID)
	}

	gormq.GetClient().Stop()
}
