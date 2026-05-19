package repository

import (
	"context"

	"github.com/local/polymarket-fetch-data-service/pkg/eventstream"
)

type EventingStore struct {
	Store
	events *eventstream.Broker
}

func NewEventingStore(base Store, events *eventstream.Broker) Store {
	if base == nil || events == nil {
		return base
	}
	return &EventingStore{Store: base, events: events}
}

func (s *EventingStore) SaveAudit(ctx context.Context, audit AuditLog) error {
	if err := s.Store.SaveAudit(ctx, audit); err != nil {
		return err
	}
	s.events.Publish(ctx, eventstream.Event{
		Type:      "audit_created",
		EntityID:  audit.EntityID,
		Payload:   map[string]any{"event": audit.Event, "payload": audit.Payload},
		CreatedAt: audit.CreatedAt,
	})
	s.events.Publish(ctx, eventstream.Event{
		Type:      "audit_log",
		EntityID:  audit.EntityID,
		Payload:   map[string]any{"event": audit.Event, "payload": audit.Payload},
		CreatedAt: audit.CreatedAt,
	})
	if audit.Event != "" {
		s.events.Publish(ctx, eventstream.Event{
			Type:      audit.Event,
			EntityID:  audit.EntityID,
			Payload:   audit.Payload,
			CreatedAt: audit.CreatedAt,
		})
	}
	return nil
}
