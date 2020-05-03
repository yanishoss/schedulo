package core

import (
	"errors"
	"math"
	"sync"
)

var ErrMaxStackCapacity = errors.New("the max stack capacity has been reached")

type stackPool []*stackNode

type stackNode struct {
	*event
	next *stackNode
}

type stack struct {
	start      *stackNode
	pool       stackPool
	len        int
	cap        int
	maxCap     int
	defaultCap int
	*sync.Mutex
}

func newStack(cap int, maxCap int) stack {
	return stack{
		pool:       allocateStackNodes(cap),
		start:      nil,
		len:        0,
		cap:        cap,
		defaultCap: cap,
		maxCap:     maxCap,
		Mutex:      &sync.Mutex{},
	}
}

func (s *stack) Push(e event) error {
	if s.len == s.cap && s.cap != s.maxCap {
		newCap := s.cap + int(math.Ceil(float64(s.maxCap-s.cap)/2))
		s.resize(newCap)
	} else if s.len == s.maxCap {
		return ErrMaxStackCapacity
	}

	s.sortedInsert(e)

	return nil
}

func (s *stack) Pop() event {
	node := s.start

	s.start = node.next

	s.pool = append(s.pool, node)

	s.len--

	if s.len == s.defaultCap {
		s.resize(s.defaultCap)
	}

	return *node.event
}

func (s *stack) resize(cap int) {
	if cap != s.cap {
		s.pool = allocateStackNodes(cap - s.len)
		s.cap = cap
	}
}

func (s *stack) Get(i int) *stackNode {
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

func (s *stack) sortedInsert(e event) {
	node := s.pool[0]
	node.event = &e
	s.pool = s.pool[1:]

	if s.start == nil {
		s.start = node
		s.len++
		return
	}

	maxIndex := s.len - 1

	left := 0
	right := maxIndex

	var ptr int

	for left <= right {
		mid := (left + right) / 2
		nd := s.Get(mid)

		if nd.ShouldExecuteAt.Before(e.ShouldExecuteAt) {
			if mid+1 <= maxIndex {
				if s.Get(mid + 1).ShouldExecuteAt.After(e.ShouldExecuteAt) {
					ptr = mid
					break
				}
			}

			if mid+1 > maxIndex {
				ptr = mid
				break
			}

			left = mid + 1
		}

		if nd.ShouldExecuteAt.After(e.ShouldExecuteAt) {
			if mid-1 == 0 {
				ptr = mid
				break
			}

			right = mid - 1
		}
	}

	ptrNode := s.Get(ptr)

	if e.ShouldExecuteAt.After(ptrNode.ShouldExecuteAt) {
		oldNext := ptrNode.next
		node.next = oldNext
		ptrNode.next = node
	} else if e.ShouldExecuteAt.Before(ptrNode.ShouldExecuteAt) || e.ShouldExecuteAt.Equal(ptrNode.ShouldExecuteAt) {
		if ptr == 0 {
			s.start = node
			node.next = ptrNode
		} else {
			beforeNode := s.Get(ptr - 1)
			beforeNode.next = node
			node.next = ptrNode
		}
	}

	s.len++
}

// allocateNodes preallocates blocks of nodes
func allocateStackNodes(n int) stackPool {
	pool := make(stackPool, 0, n)

	for i := 0; i < n; i++ {
		pool = append(pool, &stackNode{
			event: nil,
			next:  nil,
		})
	}

	return pool
}
