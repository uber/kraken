package networkevent

import "sync"

// TestProducer records all produced events.
type TestProducer struct {
	sync.Mutex
	events []Event
}

// NewTestProducer returns a new TestProducer.
func NewTestProducer() *TestProducer {
	return &TestProducer{}
}

// Produce records e.
func (p *TestProducer) Produce(e Event) error {
	p.Lock()
	defer p.Unlock()

	p.events = append(p.events, e)
	return nil
}

// Events returns all currently recorded events.
func (p *TestProducer) Events() []Event {
	p.Lock()
	defer p.Unlock()

	res := make([]Event, len(p.events))
	copy(res, p.events)
	return res
}
