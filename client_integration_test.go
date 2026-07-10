//go:build integration

package gormq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var rmqConnStr string

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Verify docker provider and daemon health
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Printf("Skipping integration tests: Docker provider not available: %v\n", err)
		os.Exit(0)
	}
	if err := provider.Health(ctx); err != nil {
		fmt.Printf("Skipping integration tests: Docker daemon is not running: %v\n", err)
		os.Exit(0)
	}

	// Spin up RabbitMQ container
	rabbitmqContainer, err := rabbitmq.Run(ctx, "rabbitmq:3-management")
	if err != nil {
		fmt.Printf("Failed to run rabbitmq container: %v\n", err)
		os.Exit(1)
	}

	connStr, err := rabbitmqContainer.AmqpURL(ctx)
	if err != nil {
		_ = rabbitmqContainer.Terminate(ctx)
		fmt.Printf("Failed to get connection string: %v\n", err)
		os.Exit(1)
	}
	rmqConnStr = connStr

	code := m.Run()

	// Clean up
	_ = rabbitmqContainer.Terminate(ctx)
	os.Exit(code)
}

func TestIntegrationPubSubAndTracing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Set up OTel Tracer
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	Init(ConnectionOptions{
		URL:                        rmqConnStr,
		ReconnectInterval:          2 * time.Second,
		FailedMessageRetryInterval: 1 * time.Second,
		MaxFailedMessageQueueSize:  10,
	})

	cl := GetClient()
	cl.StartWithContext(ctx)
	defer cl.Stop()

	// Create parent span
	ctx, parentSpan := otel.Tracer("test").Start(ctx, "publisher-parent")
	defer parentSpan.End()

	msgChan := make(chan []byte, 1)
	var extractedSpan trace.SpanContext

	_, err := cl.AddConsumerWithContext(ctx, ConsumerOption{
		Exchange:   "integration-exchange",
		RoutingKey: "integration-route",
		Queue:      "integration-queue",
		ConsumerWithContext: func(c context.Context, d []byte) error {
			extractedSpan = trace.SpanFromContext(c).SpanContext()
			msgChan <- d
			return nil
		},
		PrefetchCount: 10,
	})
	if err != nil {
		t.Fatalf("Failed to add consumer: %v", err)
	}

	// Wait for consumer subscription connection
	time.Sleep(2 * time.Second)

	testPayload := []byte(`{"hello":"world"}`)
	err = cl.PublishWithContext(ctx, NewMessage("integration-exchange", "integration-route", testPayload))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	select {
	case d := <-msgChan:
		if string(d) != string(toJson(ctx, testPayload)) {
			t.Errorf("Expected payload %s, got %s", string(toJson(ctx, testPayload)), string(d))
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for message")
	}

	// Verify trace context propagation
	if !extractedSpan.IsValid() {
		t.Error("Expected valid trace context propagated to consumer")
	}
	if extractedSpan.TraceID() != parentSpan.SpanContext().TraceID() {
		t.Errorf("Expected TraceID %s, got %s", parentSpan.SpanContext().TraceID(), extractedSpan.TraceID())
	}
}

func TestIntegrationCloseConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	Init(ConnectionOptions{
		URL:                        rmqConnStr,
		ReconnectInterval:          2 * time.Second,
		FailedMessageRetryInterval: 1 * time.Second,
	})

	cl := GetClient()
	cl.StartWithContext(ctx)
	defer cl.Stop()

	msgChan := make(chan []byte, 10)

	id, err := cl.AddConsumerWithContext(ctx, ConsumerOption{
		Exchange:   "close-test-exchange",
		RoutingKey: "close-test-route",
		Queue:      "close-test-queue",
		ConsumerWithContext: func(c context.Context, d []byte) error {
			msgChan <- d
			return nil
		},
		PrefetchCount: 10,
	})
	if err != nil {
		t.Fatalf("Failed to add consumer: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Publish first message
	_ = cl.PublishWithContext(ctx, NewMessage("close-test-exchange", "close-test-route", "msg1"))

	select {
	case <-msgChan:
		// Received msg1 successfully
	case <-ctx.Done():
		t.Fatal("Timeout waiting for first message")
	}

	// Close consumer
	if err := cl.CloseConsumerWithContext(ctx, id); err != nil {
		t.Fatalf("Failed to close consumer: %v", err)
	}

	// Publish second message
	_ = cl.PublishWithContext(ctx, NewMessage("close-test-exchange", "close-test-route", "msg2"))

	// Wait to ensure no message is delivered
	select {
	case <-msgChan:
		t.Error("Received message after consumer was closed")
	case <-time.After(3 * time.Second):
		// Success: no message received
	}
}

func TestIntegrationPublishRetryLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	Init(ConnectionOptions{
		URL:                        rmqConnStr,
		ReconnectInterval:          2 * time.Second,
		FailedMessageRetryInterval: 1 * time.Second, // retry fast for testing
		MaxFailedMessageQueueSize:  5,
	})

	cl := GetClient()

	// 1. Close underlying connection to force tryPublish to fail.
	cl.Conn().Close()
	time.Sleep(1 * time.Second)

	// This publish must fail and get enqueued
	err := cl.PublishWithContext(ctx, NewMessage("retry-exchange-invalid", "retry-route", "retry-me"))
	if err == nil {
		t.Fatal("Expected publish to fail since connection is closed")
	}

	// Re-initialize client to connect successfully
	Init(ConnectionOptions{
		URL:                        rmqConnStr,
		ReconnectInterval:          2 * time.Second,
		FailedMessageRetryInterval: 1 * time.Second,
		MaxFailedMessageQueueSize:  5,
	})

	cl = GetClient()

	// Setup consumer to catch the retried message
	msgChan := make(chan []byte, 1)
	_, err = cl.AddConsumerWithContext(ctx, ConsumerOption{
		Exchange:   "retry-exchange",
		RoutingKey: "retry-route",
		Queue:      "retry-queue",
		ConsumerWithContext: func(c context.Context, d []byte) error {
			msgChan <- d
			return nil
		},
		PrefetchCount: 10,
	})
	if err != nil {
		t.Fatalf("Failed to add consumer: %v", err)
	}

	// Simulate copying the failed message into the new client's queue
	cStruct := cl.(*client)
	cStruct.failedMsgQueue = append(cStruct.failedMsgQueue, Message{
		Exchange:   "retry-exchange",
		RoutingKey: "retry-route",
		Message:    []byte(`"retry-me"`),
	})

	// Start client, which starts the retry loop and republishes the message
	cl.StartWithContext(ctx)
	defer cl.Stop()

	// Wait for message to be retried and consumed
	select {
	case d := <-msgChan:
		if string(d) != `"retry-me"` {
			t.Errorf("Expected retried message payload 'retry-me', got %s", string(d))
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for retried message")
	}
}

func TestIntegrationTransientQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	Init(ConnectionOptions{
		URL:                        rmqConnStr,
		ReconnectInterval:          2 * time.Second,
		FailedMessageRetryInterval: 1 * time.Second,
	})

	cl := GetClient()
	cl.StartWithContext(ctx)
	defer cl.Stop()

	msgChan := make(chan []byte, 1)

	_, err := cl.AddConsumerWithContext(ctx, ConsumerOption{
		Exchange:          "transient-exchange",
		RoutingKey:        "transient-route",
		Queue:             "transient-queue",
		TransientQueue:    true,
		TransientExchange: true,
		ConsumerWithContext: func(c context.Context, d []byte) error {
			msgChan <- d
			return nil
		},
		PrefetchCount: 10,
	})
	if err != nil {
		t.Fatalf("Failed to add transient consumer: %v", err)
	}

	time.Sleep(2 * time.Second)

	testPayload := []byte(`"transient-msg"`)
	msg := NewMessage("transient-exchange", "transient-route", testPayload)
	msg.TransientExchange = true
	err = cl.PublishWithContext(ctx, msg)
	if err != nil {
		t.Fatalf("Failed to publish to transient exchange: %v", err)
	}

	select {
	case d := <-msgChan:
		if string(d) != string(toJson(ctx, testPayload)) {
			t.Errorf("Expected payload %s, got %s", string(toJson(ctx, testPayload)), string(d))
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for message on transient queue")
	}
}

func TestIntegrationAutoDeleteQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	Init(ConnectionOptions{
		URL:                        rmqConnStr,
		ReconnectInterval:          2 * time.Second,
		FailedMessageRetryInterval: 1 * time.Second,
	})

	cl := GetClient()
	cl.StartWithContext(ctx)
	defer cl.Stop()

	msgChan := make(chan []byte, 1)

	_, err := cl.AddConsumerWithContext(ctx, ConsumerOption{
		Exchange:           "autodelete-exchange",
		RoutingKey:         "autodelete-route",
		Queue:              "autodelete-queue",
		AutoDeleteQueue:    true,
		AutoDeleteExchange: true,
		ConsumerWithContext: func(c context.Context, d []byte) error {
			msgChan <- d
			return nil
		},
		PrefetchCount: 10,
	})
	if err != nil {
		t.Fatalf("Failed to add autodelete consumer: %v", err)
	}

	time.Sleep(2 * time.Second)

	testPayload := []byte(`"autodelete-msg"`)
	msg := NewMessage("autodelete-exchange", "autodelete-route", testPayload)
	msg.AutoDeleteExchange = true
	err = cl.PublishWithContext(ctx, msg)
	if err != nil {
		t.Fatalf("Failed to publish to autodelete exchange: %v", err)
	}

	select {
	case d := <-msgChan:
		if string(d) != string(toJson(ctx, testPayload)) {
			t.Errorf("Expected payload %s, got %s", string(toJson(ctx, testPayload)), string(d))
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for message on autodelete queue")
	}
}


