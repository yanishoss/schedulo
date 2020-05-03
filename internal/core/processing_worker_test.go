package core

import (
	"context"
	"testing"
	"time"
)

type _schedulerMock struct {
	count int
}

type _dispatcherMock struct {
	count int
}

func (d *_dispatcherMock) Dispatch(e event) {
	d.count++
}

func (d *_dispatcherMock) Run() {
}

func (d *_dispatcherMock) Stop() {

}

func (s *_schedulerMock) schedule(e event) {
	s.count++
}

func (s *_schedulerMock) Schedule(e Event) (ID, error) {
	s.count++
	return "", nil
}

func (s *_schedulerMock) Unschedule(id ID) error {
	return nil
}

func (s *_schedulerMock) Start() error {
	return nil
}

func (s *_schedulerMock) Stop() {
}

func (s *_schedulerMock) SetConfig(conf SchedulerConfig) error {
	return nil
}

func TestProcessingWorker(t *testing.T) {
	var config = StackManagerConfig{
		StacksNumber:         1,
		DefaultStackCapacity: 20,
	}

	s := newStackManager(config)
	sched := &_schedulerMock{}
	disp := &_dispatcherMock{}

	ev := generateEvents(10)

	for _, e := range ev {
		err := s.Push(e)

		if err != nil {
			t.Error(err)
		}
	}

	w := newProcessingWorker(context.Background(), s.stacks[0], sched, disp)

	done := false

	go func() {
		w.Run()
		done = true
	}()

	time.Sleep(time.Second * 5)

	if disp.count != 5 {
		t.Fatalf("Not every event was dispatched on time: expected:%d, got:%d\n", 5, disp.count)
	}

	time.Sleep(time.Second * 6)

	if disp.count != 10 {
		t.Fatalf("Not every event was dispatched on time: expected:%d, got:%d\n", 10, disp.count)
	}

	w.Stop()

	time.Sleep(2 * time.Second)

	if !done {
		t.Fatalf("Worker must end when stop signal is sent")
	}
}
