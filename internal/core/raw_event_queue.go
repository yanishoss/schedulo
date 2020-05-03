package core

import (
	"errors"
	"math"
	"sync"
)

var ErrMaxRawEventQueueCapacity = errors.New("the max rawEventQueue capacity has been reached")

type rawEventQueuePool []*rawEventQueueNode

type rawEventQueueNode struct {
	*Event
	next *rawEventQueueNode
}

type rawEventQueue struct {
	start      *rawEventQueueNode
	end        *rawEventQueueNode
	pool       rawEventQueuePool
	len        int
	cap        int
	maxCap     int
	defaultCap int
	*sync.Mutex
}

func newRawEventQueue(cap int, maxCap int) rawEventQueue {
	return rawEventQueue{
		pool:       allocateRawEventQueueNodes(cap),
		start:      nil,
		len:        0,
		cap:        cap,
		defaultCap: cap,
		maxCap:     maxCap,
		Mutex:      &sync.Mutex{},
	}
}

func (s *rawEventQueue) Push(e Event) error {
	if s.len == s.cap && s.cap != s.maxCap {
		newCap := s.cap + int(math.Ceil(float64(s.maxCap-s.cap)/2))
		s.resize(newCap)
	} else if s.len == s.maxCap {
		return ErrMaxEventQueueCapacity
	}

	if s.len == 0 {
		s.start = &rawEventQueueNode{
			Event: &e,
			next:  nil,
		}

		s.end = s.start
	} else {
		s.end.next = &rawEventQueueNode{
			Event: &e,
			next:  nil,
		}

		s.end = s.end.next
	}

	s.len++

	return nil
}

func (s *rawEventQueue) Pop() *Event {
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

	return node.Event
}

func (s *rawEventQueue) resize(cap int) {
	if cap != s.cap {
		s.pool = allocateRawEventQueueNodes(cap - s.len)
		s.cap = cap
	}
}

func (s *rawEventQueue) Get(i int) *rawEventQueueNode {

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
func allocateRawEventQueueNodes(n int) rawEventQueuePool {
	pool := make(rawEventQueuePool, 0, n)

	for i := 0; i < n; i++ {
		pool = append(pool, &rawEventQueueNode{
			Event: nil,
			next:  nil,
		})
	}

	return pool
}
