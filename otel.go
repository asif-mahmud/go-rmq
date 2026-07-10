package gormq

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/asif-mahmud/go-rmq"

// MapCarrier implements propagation.TextMapCarrier for map[string]interface{}
type MapCarrier map[string]interface{}

// Get returns the value associated with the key.
func (c MapCarrier) Get(key string) string {
	if val, ok := c[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// Set sets the key/value pair.
func (c MapCarrier) Set(key string, value string) {
	c[key] = value
}

// Keys returns all the keys in the carrier.
func (c MapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectTrace injects the trace context from the context into the map headers.
func InjectTrace(ctx context.Context, headers map[string]interface{}) {
	if headers == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, MapCarrier(headers))
}

// ExtractTrace extracts the trace context from the map headers.
func ExtractTrace(ctx context.Context, headers map[string]interface{}) context.Context {
	if headers == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, MapCarrier(headers))
}

// StartConsumerSpan starts a consumer span from the extracted trace context.
func StartConsumerSpan(ctx context.Context, tracer trace.Tracer, name string) (context.Context, trace.Span) {
	if tracer == nil {
		tracer = otel.Tracer(tracerName)
	}
	return tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindConsumer))
}

// StartPublisherSpan starts a publisher span from the trace context.
func StartPublisherSpan(ctx context.Context, tracer trace.Tracer, name string) (context.Context, trace.Span) {
	if tracer == nil {
		tracer = otel.Tracer(tracerName)
	}
	return tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer))
}
