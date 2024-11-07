package main

import (
	"log/slog"
	"os"
	"os/signal"
	"time"

	gormq "github.com/asif-mahmud/go-rmq"
)

type msg struct {
	Time time.Time
}

func newMsg() msg {
	return msg{
		Time: time.Now(),
	}
}

func main() {
	gormq.Init(gormq.ConnectionOptions{
		URL:                        "amqp://admin:admin@localhost:5672",
		ReconnectInterval:          10 * time.Second,
		FailedMessageRetryInterval: 1 * time.Minute,
	})

	gormq.GetClient().Start()

	gormq.GetClient().AddConsumer(gormq.ConsumerOption{
		Exchange:   "rmq-test-exchange",
		RoutingKey: "rmq-test-route",
		Queue:      "rmq-test:rmq-test-route:queue",
		Consumer: func(d []byte) error {
			// slog.Info(string(d))
			return nil
		},
		PrefetchCount: 10,
	})

	time.Sleep(5 * time.Second)

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gormq.GetClient().
				Publish(gormq.NewMessage(
					"rmq-test-exchange",
					"rmq-test-route",
					newMsg(),
				))
		case <-sigChan:
			slog.Info("Exiting")
			gormq.GetClient().Stop()
			return
		}
	}
}
