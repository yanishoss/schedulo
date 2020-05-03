package core

import "testing"

func TestAllocateNodes(t *testing.T) {
	pool := allocateStackNodes(10)

	if len(pool) != 10 {
		t.Fatalf("The pool is not of the required size: expected:%d, got:%d\n", 10, len(pool))
	}
}

func TestStack_Push(t *testing.T) {
	ev := generateEvents(21)
	s := newStack(10, 20)

	for _, e := range ev[:10] {
		if err := s.Push(e); err != nil {
			t.Error(e)
		}
	}

	for i, e := range ev[:10] {
		if !s.Get(i).ShouldExecuteAt.Equal(e.ShouldExecuteAt) {
			t.Fatalf("The stack is not sorted correctly\n")
		}
	}

	for _, e := range ev[10:20] {
		if err := s.Push(e); err != nil {
			t.Error(err)
		}
	}

	if err := s.Push(ev[20]); err == nil {
		t.Fatalf("It must throw an error as the max capacity has been reached\n")
	}
}
