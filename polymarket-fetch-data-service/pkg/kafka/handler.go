package kafka

import "context"

type HandlerFunc func(context.Context, EventEnvelope) error

type Message struct {
	Topic string
	Key   string
	Value []byte
}

type CommitFunc func()
