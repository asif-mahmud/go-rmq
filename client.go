package gormq

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Consumer defines consumer function signature
type Consumer func([]byte) error

// ConsumerWithContext defines consumer function signature that accepts context.
type ConsumerWithContext func(context.Context, []byte) error

// ConsumerOption options to run a new consumer
type ConsumerOption struct {
	Exchange            string
	RoutingKey          string
	Queue               string
	Consumer            Consumer
	ConsumerWithContext ConsumerWithContext

	PrefetchCount int

	Dlq           bool
	DlqRoutingKey string
	DlqName       string

	TransientQueue    bool
	TransientExchange bool
	AutoDeleteQueue   bool
	AutoDeleteExchange bool
}

// Message structure to publish a new message
type Message struct {
	Exchange           string
	RoutingKey         string
	Message            []byte
	TransientExchange  bool
	AutoDeleteExchange bool
}

func toJson(ctx context.Context, data any) []byte {
	d, e := json.Marshal(data)
	if e != nil {
		slog.ErrorContext(ctx, "failed to marshal message payload", "error", e)
		return []byte(``)
	}
	return d
}

// NewMessageWithContext creates a new Message instance with context propagation for marshaling logs.
func NewMessageWithContext(ctx context.Context, exchange, routingKey string, data interface{}) Message {
	dataStr := toJson(ctx, data)
	return Message{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Message:    []byte(dataStr),
	}
}

// NewMessage creates a new Message instance.
// This is an utility function to create a new message instance.
func NewMessage(exchange, routingKey string, data interface{}) Message {
	return NewMessageWithContext(context.Background(), exchange, routingKey, data)
}

// Client interface for a rabbitmq pub/sub client
type Client interface {
	// Start runs retry routine.
	// This does not block execution.
	// This should be called only once.
	Start()

	// StartWithContext runs retry routine with a context.
	// This does not block execution.
	// This should be called only once.
	StartWithContext(ctx context.Context)

	// Stop stops retry routine and consumers.
	// This also cleans up publishers and finally stops
	// the connection.
	// This should be called only once. After calling Stop
	// a client should not be used anymore, create a new client if
	// you have to.
	Stop()

	// Conn returns underlying rabbitmq.Conn
	Conn() *rabbitmq.Conn

	// AddConsumer adds and starts consuming
	AddConsumer(ConsumerOption) error

	// AddConsumerWithContext adds and starts consuming with context.
	// Returns a unique consumer identifier.
	AddConsumerWithContext(ctx context.Context, opt ConsumerOption) (string, error)

	// CloseConsumer closes a specific consumer by ID.
	CloseConsumer(id string) error

	// CloseConsumerWithContext closes a specific consumer by ID with context.
	CloseConsumerWithContext(ctx context.Context, id string) error

	// Publish publishes message, on failure enqueues for retry
	Publish(Message)

	// PublishWithContext publishes message with context.
	PublishWithContext(ctx context.Context, msg Message) error
}

type client struct {
	serverUrl string // rabbitmq server url

	conn *rabbitmq.Conn

	publishers  map[string]*rabbitmq.Publisher // [route]*rabbitmq.Publisher
	publisersMu sync.Mutex

	consumers   map[string]*rabbitmq.Consumer // [id]*rabbitmq.Consumer
	consumersMu sync.RWMutex

	failedMsgQueue []Message
	failedMsgMu    sync.Mutex

	failedMsgRetryInterval time.Duration

	failedMsgStopChan chan struct{}

	maxFailedMsgQueueSize int
	consumerSeq           uint64

	cancelRetryLoop context.CancelFunc
	logLevel        slog.Level
	netPeerName     string
}

func (c *client) createDlq(opt ConsumerOption) error {
	conn, err := amqp.Dial(c.serverUrl)
	if err != nil {
		return err
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	// declare exchange
	if err := channel.ExchangeDeclare(
		opt.Exchange,           // exchange
		"topic",                // kind
		!opt.TransientExchange, // durable
		opt.AutoDeleteExchange, // autoDelete
		false,                  // internal
		false,                  // nowait
		nil,                    // args
	); err != nil {
		return err
	}

	// declare queue
	if _, err := channel.QueueDeclare(
		opt.DlqName,            // name
		!opt.TransientQueue,    // durable
		opt.AutoDeleteQueue,    // autoDelete
		false,                  // exclusive
		false,                  // nowait
		nil,                    // args
	); err != nil {
		return err
	}

	// bind queue
	if err := channel.QueueBind(
		opt.DlqName,       // name
		opt.DlqRoutingKey, // key
		opt.Exchange,      // exchange
		false,             // noWait
		nil,               // args
	); err != nil {
		return err
	}

	return nil
}

func (c *client) runConsumer(ctx context.Context, opt ConsumerOption) (*rabbitmq.Consumer, error) {
	opts := []func(*rabbitmq.ConsumerOptions){
		rabbitmq.WithConsumerOptionsLogger(&Logger{Level: c.logLevel}),
		rabbitmq.WithConsumerOptionsRoutingKey(opt.RoutingKey),
		rabbitmq.WithConsumerOptionsConsumerAutoAck(false),
		rabbitmq.WithConsumerOptionsExchangeName(opt.Exchange),
		rabbitmq.WithConsumerOptionsExchangeKind("topic"),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
		rabbitmq.WithConsumerOptionsQOSPrefetch(opt.PrefetchCount),
	}

	if !opt.TransientQueue {
		opts = append(opts, rabbitmq.WithConsumerOptionsQueueDurable)
	}
	if !opt.TransientExchange {
		opts = append(opts, rabbitmq.WithConsumerOptionsExchangeDurable)
	}
	if opt.AutoDeleteQueue {
		opts = append(opts, rabbitmq.WithConsumerOptionsQueueAutoDelete)
	}
	if opt.AutoDeleteExchange {
		opts = append(opts, rabbitmq.WithConsumerOptionsExchangeAutoDelete)
	}

	consumer, err := rabbitmq.NewConsumer(
		c.conn,
		opt.Queue,
		opts...,
	)
	if err != nil {
		return nil, err
	}

	go consumer.Run(func(d rabbitmq.Delivery) (action rabbitmq.Action) {
		msgCtx := extractTrace(ctx, d.Headers)

		var span trace.Span
		if opt.ConsumerWithContext != nil {
			msgCtx, span = startConsumerSpan(msgCtx, nil, "Consume "+opt.Queue, attribute.String(TraceNetPeerNameKey, c.netPeerName))
			defer span.End()
		}

		var runErr error
		if opt.ConsumerWithContext != nil {
			runErr = opt.ConsumerWithContext(msgCtx, d.Body)
		} else if opt.Consumer != nil {
			runErr = opt.Consumer(d.Body)
		}

		if runErr == nil {
			return rabbitmq.Ack
		}

		if span != nil {
			span.RecordError(runErr)
		}

		if opt.Dlq {
			_ = c.PublishWithContext(msgCtx, NewMessage(opt.Exchange, opt.DlqRoutingKey, d.Body))
		}

		return rabbitmq.Ack
	})

	return consumer, nil
}

// AddConsumer implements Client.
func (c *client) AddConsumer(opt ConsumerOption) error {
	_, err := c.AddConsumerWithContext(context.Background(), opt)
	return err
}

// AddConsumerWithContext implements Client.
func (c *client) AddConsumerWithContext(ctx context.Context, opt ConsumerOption) (string, error) {
	if opt.Dlq && (opt.DlqName == "" || opt.DlqRoutingKey == "") {
		return "", fmt.Errorf(
			"dlq name and routing key is required. got, dlq: %s, dlq route: %s",
			opt.DlqName,
			opt.DlqRoutingKey,
		)
	}

	if opt.Dlq {
		if err := c.createDlq(opt); err != nil {
			return "", err
		}
	}

	consumer, err := c.runConsumer(ctx, opt)
	if err != nil {
		return "", err
	}

	// Generate random short ID + sequence to ensure uniqueness
	randBytes := make([]byte, 4)
	_, _ = rand.Read(randBytes)
	seq := atomic.AddUint64(&c.consumerSeq, 1)
	id := fmt.Sprintf("%x-%d", randBytes, seq)

	c.consumersMu.Lock()
	c.consumers[id] = consumer
	c.consumersMu.Unlock()

	return id, nil
}

// CloseConsumer implements Client.
func (c *client) CloseConsumer(id string) error {
	return c.CloseConsumerWithContext(context.Background(), id)
}

// CloseConsumerWithContext implements Client.
func (c *client) CloseConsumerWithContext(ctx context.Context, id string) error {
	c.consumersMu.Lock()
	consumer, ok := c.consumers[id]
	if !ok {
		c.consumersMu.Unlock()
		return fmt.Errorf("consumer not found: %s", id)
	}
	delete(c.consumers, id)
	c.consumersMu.Unlock()

	consumer.Close()
	return nil
}

// Conn implements Client.
func (c *client) Conn() *rabbitmq.Conn {
	return c.conn
}

func (c *client) createPublisher(ctx context.Context, exchange string, route string, transient, autoDelete bool) (*rabbitmq.Publisher, error) {
	opts := []func(*rabbitmq.PublisherOptions){
		rabbitmq.WithPublisherOptionsLogger(&Logger{Level: c.logLevel}),
		rabbitmq.WithPublisherOptionsExchangeName(exchange),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsExchangeKind("topic"),
	}

	if !transient {
		opts = append(opts, rabbitmq.WithPublisherOptionsExchangeDurable)
	}
	if autoDelete {
		opts = append(opts, rabbitmq.WithPublisherOptionsExchangeAutoDelete)
	}

	publisher, err := rabbitmq.NewPublisher(
		c.conn,
		opts...,
	)
	if err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, fmt.Sprintf("created new publisher. exchange: %s, route: %s, transient: %v, autoDelete: %v", exchange, route, transient, autoDelete))

	return publisher, nil
}

func (c *client) tryPublish(ctx context.Context, msg Message) error {
	headers := make(map[string]interface{})
	injectTrace(ctx, headers)

	c.publisersMu.Lock()
	publisher, ok := c.publishers[msg.RoutingKey]
	var err error
	if !ok {
		publisher, err = c.createPublisher(ctx, msg.Exchange, msg.RoutingKey, msg.TransientExchange, msg.AutoDeleteExchange)
		if err != nil {
			c.publisersMu.Unlock()
			slog.ErrorContext(
				ctx,
				fmt.Sprintf(
					"failed to create publisher. exchange: %s, route: %s, error: %s",
					msg.Exchange,
					msg.RoutingKey,
					err.Error(),
				),
			)
			return err
		}
		c.publishers[msg.RoutingKey] = publisher
	}
	c.publisersMu.Unlock()

	return publisher.Publish(
		msg.Message,
		[]string{msg.RoutingKey},
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsExchange(msg.Exchange),
		rabbitmq.WithPublishOptionsHeaders(rabbitmq.Table(headers)),
	)
}

// Publish implements Client.
func (c *client) Publish(msg Message) {
	_ = c.PublishWithContext(context.Background(), msg)
}

// PublishWithContext implements Client.
func (c *client) PublishWithContext(ctx context.Context, msg Message) error {
	ctx, span := startPublisherSpan(ctx, nil, "Publish "+msg.RoutingKey, attribute.String(TraceNetPeerNameKey, c.netPeerName))
	defer span.End()

	err := c.tryPublish(ctx, msg)
	if err == nil {
		return nil
	}

	if span != nil {
		span.RecordError(err)
	}

	c.failedMsgMu.Lock()
	defer c.failedMsgMu.Unlock()

	if len(c.failedMsgQueue) >= c.maxFailedMsgQueueSize {
		slog.WarnContext(
			ctx,
			fmt.Sprintf(
				"failed message queue limit reached (%d). discarding message for route: %s. error: %s",
				c.maxFailedMsgQueueSize,
				msg.RoutingKey,
				err.Error(),
			),
		)
		return fmt.Errorf("failed message queue full: %w", err)
	}

	c.failedMsgQueue = append(c.failedMsgQueue, msg)

	slog.InfoContext(
		ctx,
		fmt.Sprintf(
			"enqueued failed message. count: %d, limit: %d, error: %s",
			len(c.failedMsgQueue),
			c.maxFailedMsgQueueSize,
			err.Error(),
		),
	)

	return err
}

func (c *client) startRetryLoop(ctx context.Context) {
	ticker := time.NewTicker(c.failedMsgRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.DebugContext(ctx, "stopping failed message retry loop due to context cancel")
			return

		case <-c.failedMsgStopChan:
			slog.DebugContext(ctx, "stopping failed message retry loop via stop channel")
			return

		case <-ticker.C:
			c.failedMsgMu.Lock()
			if len(c.failedMsgQueue) == 0 {
				c.failedMsgMu.Unlock()
				break
			}
			toRetry := make([]Message, len(c.failedMsgQueue))
			copy(toRetry, c.failedMsgQueue)
			c.failedMsgMu.Unlock()

			failedAgain := []Message{}
			for _, msg := range toRetry {
				if err := c.tryPublish(ctx, msg); err != nil {
					failedAgain = append(failedAgain, msg)
				}
			}

			c.failedMsgMu.Lock()
			if len(failedAgain) > 0 {
				totalLen := len(failedAgain) + len(c.failedMsgQueue) - len(toRetry)
				if totalLen > c.maxFailedMsgQueueSize {
					slog.WarnContext(ctx, "failed message queue full after retry, trimming oldest messages")
					availableSpace := c.maxFailedMsgQueueSize - len(c.failedMsgQueue) + len(toRetry)
					if availableSpace > 0 {
						if len(failedAgain) > availableSpace {
							failedAgain = failedAgain[:availableSpace]
						}
					} else {
						failedAgain = []Message{}
					}
				}

				if len(c.failedMsgQueue) >= len(toRetry) {
					c.failedMsgQueue = append(failedAgain, c.failedMsgQueue[len(toRetry):]...)
				} else {
					c.failedMsgQueue = failedAgain
				}
			} else {
				if len(c.failedMsgQueue) >= len(toRetry) {
					c.failedMsgQueue = c.failedMsgQueue[len(toRetry):]
				} else {
					c.failedMsgQueue = []Message{}
				}
			}

			slog.InfoContext(ctx, fmt.Sprintf("retried failed messages. failed again: %d", len(failedAgain)))
			c.failedMsgMu.Unlock()
		}
	}
}

func (c *client) stopRetryLoop() {
	if c.cancelRetryLoop != nil {
		c.cancelRetryLoop()
	}
	select {
	case c.failedMsgStopChan <- struct{}{}:
	default:
	}
}

// Start implements Client.
func (c *client) Start() {
	c.StartWithContext(context.Background())
}

// StartWithContext implements Client.
func (c *client) StartWithContext(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)
	c.cancelRetryLoop = cancel
	go c.startRetryLoop(loopCtx)
}

// Stop implements Client.
func (c *client) Stop() {
	c.stopRetryLoop()

	c.consumersMu.Lock()
	defer c.consumersMu.Unlock()
	for _, consumer := range c.consumers {
		consumer.Close()
	}
	c.consumers = map[string]*rabbitmq.Consumer{}

	c.publisersMu.Lock()
	defer c.publisersMu.Unlock()
	for _, publisher := range c.publishers {
		publisher.Close()
	}
	c.publishers = map[string]*rabbitmq.Publisher{}

	c.conn.Close()

	slog.InfoContext(context.Background(), "all cleaned up. client stopped.")
}

var _ = (Client)(&client{})
