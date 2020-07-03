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
			ShouldExecuteAt: time.Now(),
		}
	}

	return ev
}

func TestServer(t *testing.T) {
	addr := os.Getenv("SCHEDULO_ADDR")

	t.Logf("Trying to connect to %s server\n", addr)
	cl, err := schedulo.New(addr)
	defer cl.Close()

	if err != nil {
		t.Errorf("An error occurred while creating the client %v\n", err)
	}

	t.Logf("Created client...\n")

	dispatched := 0

	close_, err := cl.OnEvent(context.Background(), "test", func(e core.Event) {
		dispatched++
		t.Log("Event dispatched")
	}, func (err1 error) {
		if err1 != nil {
			err = err1
		}
	})

	if err != nil {
		t.Errorf("An error occurred while listening to events: %v\n", err)
	}

	start := time.Now()
	for _, e := range ev {
		go func() {
			if _, err := cl.Schedule(context.Background(), e); err != nil {
				t.Errorf("An error occurred while scheduling event: %v\n", err)
			}
			t.Log("Event scheduled...")
		}()
	}

	dur := time.Now().Sub(start).Seconds()
	t.Logf("it took %.2f to dispatch 10 events\n", dur)


	ctx , _ := context.WithTimeout(context.Background(), time.Minute)
	for dispatched != 10 {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
	}

	if dispatched != 10 {
		t.Fatalf("Not every events got dispatched: expected: 10, got: %d\n", dispatched)
	}

	close_()
}
