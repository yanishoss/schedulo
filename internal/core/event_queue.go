package core

import (
	"errors"
	"math"
	"sync"
)

var ErrMaxEventQueueCapacity = errors.New("the max eventQueue capacity has been reached")

type eventQueuePool []*eventQueueNode

type eventQueueNode struct {
	*event
	next *eventQueueNode
}

type eventQueue struct {
	start      *eventQueueNode
	end        *eventQueueNode
	pool       eventQueuePool
	len        int
	cap        int
	maxCap     int
	defaultCap int
	*sync.Mutex
}

func newEventQueue(cap int, maxCap int) eventQueue {
	return eventQueue{
		pool:       allocateEventQueueNodes(cap),
		start:      nil,
		len:        0,
		cap:        cap,
		defaultCap: cap,
		maxCap:     maxCap,
		Mutex:      &sync.Mutex{},
	}
}

func (s *eventQueue) Push(e event) error {
	if s.len == s.cap && s.cap != s.maxCap {
		newCap := s.cap + int(math.Ceil(float64(s.maxCap-s.cap)/2))
		s.resize(newCap)
	} else if s.len == s.maxCap {
		return ErrMaxEventQueueCapacity
	}

	if s.len == 0 {
		s.start = &eventQueueNode{
			event: &e,
			next:  nil,
		}

		s.end = s.start
	} else {
		s.end.next = &eventQueueNode{
			event: &e,
			next:  nil,
		}

		s.end = s.end.next
	}

	s.len++

	return nil
}

func (s *eventQueue) Pop() *event {
	node := s.start

	if node == nil {
		return nil
	}

	s.start = node.next

	s.pool = append(s.pool, node)

	s.len--

	if s.len == s.defaultCap {
		s.resize(s.defaultCap)
	}

	return node.event
}

func (s *eventQueue) resize(cap int) {
	if cap != s.cap {
		s.pool = allocateEventQueueNodes(cap - s.len)
		s.cap = cap
	}
}

func (s *eventQueue) Get(i int) *eventQueueNode {

	node := s.start

	j := 0
	for node != nil {
		if i == j {
			break
		}

		node = node.next
		j++
	}

	return node
}

// allocateNodes preallocates blocks of nodes
func allocateEventQueueNodes(n int) eventQueuePool {
	pool := make(eventQueuePool, 0, n)

	for i := 0; i < n; i++ {
		pool = append(pool, &eventQueueNode{
			event: nil,
			next:  nil,
		})
	}

	return pool
}
