package core

import (
	"math/rand"
	"testing"
	"time"
)

var config = StackManagerConfig{
	StacksNumber:         20000,
	DefaultStackCapacity: 50,
	MaxStackCapacity:     100,
}

var ev = generateEvents(2 * config.StacksNumber * config.DefaultStackCapacity)

func TestStackManager(t *testing.T) {
	var config = StackManagerConfig{
		StacksNumber:         5,
		DefaultStackCapacity: 300,
	}

	s := newStackManager(config)

	if len(s.stacks) != config.StacksNumber {
		t.Fatalf("The stack manager doesn't have enough stacks: expect %d, got %d\n", config.StacksNumber, s.stacks.Len())
	}

	if s.stacks[0].cap < config.DefaultStackCapacity {
		t.Fatalf("The stack manager doesn't have enough stack capacity: expect %d, got %d\n", config.DefaultStackCapacity, s.stacks[0].len)
	}

	ev := generateEvents(config.DefaultStackCapacity*config.StacksNumber + 10)

	for i, e := range ev {
		if i == config.DefaultStackCapacity*config.StacksNumber-1 {
			break
		}

		_ = s.Push(e)
	}

	for i := 0; i < s.stacks[0].len; i++ {
		ev := s.stacks[0].Get(i)

		if i-1 >= 0 {
			evBefore := s.stacks[0].Get(i - 1)

			if ev.ShouldExecuteAt.Before(evBefore.ShouldExecuteAt) {
				t.Fatalf("The stack must be sorted timestamp ascending wise\n")
			}
		}

		if i+1 <= s.stacks[0].len-1 {
			evAfter := s.stacks[0].Get(i + 1)

			if ev.ShouldExecuteAt.After(evAfter.ShouldExecuteAt) {
				t.Fatalf("The stack must be sorted timestamp ascending wise\n")
			}
		}
	}

	for i, e := range ev[config.DefaultStackCapacity*config.StacksNumber-1:] {
		if i == config.DefaultStackCapacity*config.StacksNumber-1 {
			break
		}

		_ = s.Push(e)
	}

	if s.stacks[0].cap <= config.DefaultStackCapacity {
		t.Fatalf("The stack capacity must be expanded as new events have been pushed: %d capacity\n", s.stacks[0].cap)
	}

	if err := s.resize(7); err != nil {
		t.Error(err)
	}

	if len(s.stacks) != 7 {
		t.Fatalf("The number of stack must be resized: expect:%d, got:%d\n", 7, len(s.stacks))
	}

	oldLen := s.Len()

	if err := s.resize(2); err != nil {
		t.Error(err)
	}

	if s.stacks.Len() != 2 {
		t.Fatalf("The stack manager has not been resized: expected:%d, got:%d", 2, s.stacks.Len())
	}

	if s.Len() != oldLen {
		t.Fatalf("Elements have been lost while resizing the stack manager: expected:%d, got:%d\n", oldLen, s.Len())
	}

	ev = generateEvents(2*maxDefaultStackCapacity - len(ev) + 1)

	var err1 error
	for _, e := range ev {
		err := s.Push(e)

		if err != nil {
			err1 = err
		}
	}

	if err1 == nil {
		t.Fatalf("Max capacity has been reached but no error was raised\n")
	} else {
		t.Logf("This error code is normal: %s\n", err1.Error())
	}

	if err := s.resize(1); err == nil {
		t.Fatalf("Max capacity has already been reached but no error was raised\n")
	}
}

func BenchmarkInsertingBelowCapacity(b *testing.B) {
	b.StopTimer()
	s := newStackManager(config)

	smpl := ev[:config.DefaultStackCapacity*config.StacksNumber]

	b.StartTimer()
	for _, e := range smpl {
		err := s.Push(e)

		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkInsertingPastCapacity(b *testing.B) {
	b.StopTimer()
	s := newStackManager(config)

	smpl := ev[:config.StacksNumber*config.DefaultStackCapacity-1]

	for _, e := range smpl {
		err := s.Push(e)

		if err != nil {
			b.Error(err)
		}
	}

	smpl2 := ev[config.StacksNumber*config.DefaultStackCapacity:]

	b.StartTimer()
	for _, e := range smpl2 {
		err := s.Push(e)

		if err != nil {
			b.Error(err)
		}
	}
}

func randomID(n int) ID {
	const CHARACTERS = "AZERTYUIOPQSDFGHJKLMWXCVBN1234567890"

	str := ""

	for i := 0; i < n; i++ {
		j := rand.Intn(len(CHARACTERS) - 1)

		str += string(CHARACTERS[j])
	}

	return ID(str)
}

func generateEvents(n int) []event {
	const idSize = 8

	ev := make([]event, n)

	for i := 0; i < n; i++ {
		ev[i] = event{
			Mode:            TimestampMode,
			ShouldExecuteAt: time.Now().Add(time.Duration(time.Second.Nanoseconds() * int64(i+1))),
			ID:              randomID(idSize),
		}
	}

	return ev
}
