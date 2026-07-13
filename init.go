package gormq

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
)

const DefaultMaxFailedMessageQueueSize = 5000

type ConnectionOptions struct {
	URL                        string
	ReconnectInterval          time.Duration
	FailedMessageRetryInterval time.Duration
	ClientName                 string
	MaxFailedMessageQueueSize  int
	LogLevel                   *slog.Level
	NetPeerName                string
}

var DefaultClient Client

// Init initializes rabbitmq client.
// On success, it sets the DefaultClient to established client.
// On failure, it exits the application instance.
func Init(opt ConnectionOptions) {
	logLevel := slog.LevelInfo
	if opt.LogLevel != nil {
		logLevel = *opt.LogLevel
	}

	opts := [](func(options *rabbitmq.ConnectionOptions)){
		rabbitmq.WithConnectionOptionsLogger(&Logger{Level: logLevel}),
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
		slog.ErrorContext(context.Background(), fmt.Sprintf("failed to connect to rabbitmq, error: %s", err.Error()))
		os.Exit(1)
	}

	maxFailedMsgSize := opt.MaxFailedMessageQueueSize
	if maxFailedMsgSize <= 0 {
		maxFailedMsgSize = DefaultMaxFailedMessageQueueSize
	}

	netPeerName := opt.NetPeerName
	if netPeerName == "" {
		netPeerName = "rabbitmq"
	}

	DefaultClient = &client{
		serverUrl:              opt.URL,
		conn:                   conn,
		publishers:             map[string]*rabbitmq.Publisher{},
		consumers:              map[string]*rabbitmq.Consumer{},
		failedMsgQueue:         []Message{},
		failedMsgRetryInterval: opt.FailedMessageRetryInterval,
		failedMsgStopChan:      make(chan struct{}),
		maxFailedMsgQueueSize:  maxFailedMsgSize,
		logLevel:               logLevel,
		netPeerName:            netPeerName,
	}
}

// GetClient returns DefaultClient.
// This should only be used after a successfull Init call.
func GetClient() Client {
	return DefaultClient
}
