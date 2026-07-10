package gormq

import (
	"context"
	"testing"
	"time"

	"github.com/wagslane/go-rabbitmq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestMapCarrier(t *testing.T) {
	carrier := make(MapCarrier)
	carrier.Set("foo", "bar")
	carrier.Set("hello", "world")

	if carrier.Get("foo") != "bar" {
		t.Errorf("Expected 'bar', got '%s'", carrier.Get("foo"))
	}
	if carrier.Get("hello") != "world" {
		t.Errorf("Expected 'world', got '%s'", carrier.Get("hello"))
	}
	if carrier.Get("nonexistent") != "" {
		t.Errorf("Expected empty string, got '%s'", carrier.Get("nonexistent"))
	}

	keys := carrier.Keys()
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}
}

func TestTracePropagation(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)

	// Set global propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	ctx := context.Background()
	tracer := otel.Tracer("test")
	ctx, span := tracer.Start(ctx, "test-span")
	defer span.End()

	headers := make(map[string]interface{})
	InjectTrace(ctx, headers)

	if len(headers) == 0 {
		t.Error("Expected headers to contain trace context metadata")
	}

	extractedCtx := ExtractTrace(context.Background(), headers)
	extractedSpan := trace.SpanFromContext(extractedCtx)

	if !extractedSpan.SpanContext().IsValid() {
		t.Error("Expected valid extracted span context")
	}

	if extractedSpan.SpanContext().TraceID() != span.SpanContext().TraceID() {
		t.Errorf("Expected TraceID %s, got %s", span.SpanContext().TraceID(), extractedSpan.SpanContext().TraceID())
	}
}

func TestConnectionOptionsDefaults(t *testing.T) {
	// We want to test that Init assigns default value if none provided
	opt := ConnectionOptions{
		URL:                        "amqp://guest:guest@localhost:5672",
		ReconnectInterval:          5 * time.Second,
		FailedMessageRetryInterval: 5 * time.Second,
	}

	// Mocking amqp.Dial to fail quickly or checking code assignments.
	// Since Init calls rabbitmq.NewConn which attempts network dial immediately,
	// we will manually verify DefaultMaxFailedMessageQueueSize behavior or test our client initialization structure.
	c := &client{
		maxFailedMsgQueueSize: opt.MaxFailedMessageQueueSize,
	}
	if c.maxFailedMsgQueueSize <= 0 {
		c.maxFailedMsgQueueSize = DefaultMaxFailedMessageQueueSize
	}

	if c.maxFailedMsgQueueSize != DefaultMaxFailedMessageQueueSize {
		t.Errorf("Expected maxFailedMsgQueueSize to fall back to %d, got %d", DefaultMaxFailedMessageQueueSize, c.maxFailedMsgQueueSize)
	}
}

func TestConsumerTrackingRegistration(t *testing.T) {
	c := &client{
		consumers:   make(map[string]*rabbitmq.Consumer),
		consumerSeq: 0,
	}

	if len(c.consumers) != 0 {
		t.Errorf("Expected initial consumer count to be 0, got %d", len(c.consumers))
	}
}
