package gormq

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
)

type ConnectionOptions struct {
	URL                        string
	ReconnectInterval          time.Duration
	FailedMessageRetryInterval time.Duration
	ClientName                 string
}

var DefaultClient Client

// Init initializes rabbitmq client.
// On success, it sets the DefaultClient to established client.
// On failure, it exits the application instance.
func Init(opt ConnectionOptions) {
	opts := [](func(options *rabbitmq.ConnectionOptions)){
		rabbitmq.WithConnectionOptionsLogger(&Logger{}),
		rabbitmq.WithConnectionOptionsReconnectInterval(opt.ReconnectInterval),
	}

	if len(opt.ClientName) > 0 {
		props := amqp091.NewConnectionProperties()
		props.SetClientConnectionName(opt.ClientName)
		cfg := rabbitmq.Config{
			Properties: props,
		}
		opts = append(opts, rabbitmq.WithConnectionOptionsConfig(cfg))
	}

	conn, err := rabbitmq.NewConn(
		opt.URL,
		opts...,
	)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to connect to rabbitmq, error: %s", err.Error()))
		os.Exit(1)
	}

	DefaultClient = &client{
		serverUrl:              opt.URL,
		conn:                   conn,
		publishers:             map[string]*rabbitmq.Publisher{},
		consumers:              []*rabbitmq.Consumer{},
		failedMsgQueue:         []Message{},
		failedMsgRetryInterval: opt.FailedMessageRetryInterval,
		failedMsgStopChan:      make(chan bool),
	}
}

// GetClient returns DefaultClient.
// This should only be used after a successfull Init call.
func GetClient() Client {
	return DefaultClient
}
