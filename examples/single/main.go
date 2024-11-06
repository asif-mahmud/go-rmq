package main

import (
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

	gormq.GetClient().Start()

	dataChan := make(chan []byte)

	gormq.GetClient().AddConsumer(gormq.ConsumerOption{
		Exchange:   "rmq-test-exchange",
		RoutingKey: "rmq-test-route",
		Queue:      "rmq-test:rmq-test-route:queue",
		Consumer: func(d []byte) error {
			dataChan <- d
			slog.Info(string(d))
			return nil
		},
		PrefetchCount: 10,
	})

	time.Sleep(5 * time.Second)

	gormq.GetClient().
		Publish(gormq.NewMessage(
			"rmq-test-exchange",
			"rmq-test-route",
			`{"success":true}`,
		))

	msg := <-dataChan
	slog.Info(fmt.Sprintf("Received: %s", string(msg)))

	gormq.GetClient().Stop()
}
