package gormq

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/asif-mahmud/go-rmq"

// mapCarrier implements propagation.TextMapCarrier for map[string]interface{}
type mapCarrier map[string]interface{}

// Get returns the value associated with the key.
func (c mapCarrier) Get(key string) string {
	if val, ok := c[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// Set sets the key/value pair.
func (c mapCarrier) Set(key string, value string) {
	c[key] = value
}

// Keys returns all the keys in the carrier.
func (c mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// injectTrace injects the trace context from the context into the map headers.
func injectTrace(ctx context.Context, headers map[string]interface{}) {
	if headers == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, mapCarrier(headers))
}

// extractTrace extracts the trace context from the map headers.
func extractTrace(ctx context.Context, headers map[string]interface{}) context.Context {
	if headers == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, mapCarrier(headers))
}

// startConsumerSpan starts a consumer span from the extracted trace context.
func startConsumerSpan(ctx context.Context, tracer trace.Tracer, name string) (context.Context, trace.Span) {
	if tracer == nil {
		tracer = otel.Tracer(tracerName)
	}
	return tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindConsumer))
}

// startPublisherSpan starts a publisher span from the trace context.
func startPublisherSpan(ctx context.Context, tracer trace.Tracer, name string) (context.Context, trace.Span) {
	if tracer == nil {
		tracer = otel.Tracer(tracerName)
	}
	return tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer))
}
