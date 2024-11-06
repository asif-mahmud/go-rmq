package main

import (
	"errors"
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

	successChan := make(chan bool, 1)
	success := 0
	failChan := make(chan bool, 1)
	fail := 0

	gormq.GetClient().AddConsumer(gormq.ConsumerOption{
		Exchange:   "rmq-test-exchange",
		RoutingKey: "rmq-test-route",
		Queue:      "rmq-test:rmq-test-route:queue",
		Consumer: func(d []byte) error {
			failChan <- true
			slog.Info(fmt.Sprintf("Failing: %s", string(d)))
			return errors.New("failed test")
		},
		PrefetchCount: 1,
		Dlq:           true,
		DlqName:       "rmq-test:rmq-test-route:dlq",
		DlqRoutingKey: "rmq-test:rmq-test-route:dlq",
	})

	gormq.GetClient().AddConsumer(gormq.ConsumerOption{
		Exchange:   "rmq-test-exchange",
		RoutingKey: "rmq-test-route",
		Queue:      "rmq-test:rmq-test-route:queue",
		Consumer: func(d []byte) error {
			successChan <- true
			slog.Info(string(d))
			return nil
		},
		PrefetchCount: 1,
	})

	time.Sleep(10 * time.Second)

	for i := 0; i < 100; i++ {
		gormq.GetClient().
			Publish(gormq.NewMessage(
				"rmq-test-exchange",
				"rmq-test-route",
				`{"success":true}`,
			))
	}

	for {
		select {
		case <-successChan:
			success += 1

		case <-failChan:
			fail += 1
		}

		if success+fail == 100 {
			break
		}
	}
	slog.Info(fmt.Sprintf("succeeded: %d, failed: %d", success, fail))

	time.Sleep(15 * time.Second)

	gormq.GetClient().Stop()
}
