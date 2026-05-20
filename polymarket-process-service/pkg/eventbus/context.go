package eventbus

import "context"

type pipelinePublishKey struct{}

func WithoutPipelineEvents(ctx context.Context) context.Context {
	return context.WithValue(ctx, pipelinePublishKey{}, false)
}

func PipelineEventsEnabled(ctx context.Context) bool {
	enabled, ok := ctx.Value(pipelinePublishKey{}).(bool)
	return !ok || enabled
}
