package service

type seenTracker struct {
	max   int
	order []string
	items map[string]struct{}
}

func newSeenTracker(max int) *seenTracker {
	return &seenTracker{max: max, items: map[string]struct{}{}}
}

func (s *seenTracker) Has(id string) bool {
	_, ok := s.items[id]
	return ok
}

func (s *seenTracker) Add(id string) {
	if id == "" {
		return
	}
	if _, ok := s.items[id]; ok {
		return
	}
	s.items[id] = struct{}{}
	s.order = append(s.order, id)
	for s.max > 0 && len(s.order) > s.max {
		delete(s.items, s.order[0])
		s.order = s.order[1:]
	}
}
