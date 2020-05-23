package main

import (
	"context"
	"github.com/yanishoss/schedulo/internal/core"
	"github.com/yanishoss/schedulo/pkg/schedulo"
	"os"
	"testing"
	"time"
)

var ev = generateEvents(10)

func generateEvents(n int) []core.Event {
	ev := make([]core.Event, n)

	for i := 0; i < n; i++ {
		ev[i] = core.Event{
			Mode:            core.TimestampMode,
			Topic: "test",
			ShouldExecuteAt: time.Now().Add(time.Duration(time.Second.Nanoseconds() * int64(i+1))),
		}
	}

	return ev
}

func TestServer(t *testing.T) {
	cl, err := schedulo.New(os.Getenv("SCHEDULO_ADDR"))
	defer cl.Close()

	if err != nil {
		t.Errorf("An error occurred while creating the client %v\n", err)
	}

	t.Logf("Created client...\n")

	dispatched := 0

	close_, err := cl.OnEvent(context.Background(), "test", func(e core.Event) {
		dispatched++
	}, func (err1 error) {
		if err1 != nil {
			err = err1
		}
	})

	if err != nil {
		t.Errorf("An error occurred while listening to events: %v\n", err)
	}

	for _, e := range ev {
		go func(){
			if _, err := cl.Schedule(context.Background(), e); err != nil {
				t.Errorf("An error occurred while scheduling event: %v\n", err)
			}
		}()
	}

	start := time.Now()
	for dispatched != 10 {
		if time.Now().Sub(start) > time.Minute*2 {
			break
		}
	}

	if dispatched != 10 {
		t.Fatalf("Not every events got dispatched: expected: 10, got: %d\n", dispatched)
	}

	close_()
}
