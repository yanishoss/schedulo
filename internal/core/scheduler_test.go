package core

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func generateScheduledEvents(n int) []Event {
	ev := make([]Event, n)

	for i := 0; i < n; i++ {
		ev[i] = Event{
			Mode:            TimestampMode,
			ShouldExecuteAt: time.Now().Add(time.Duration(time.Millisecond.Nanoseconds() * int64(i+1))),
			Payload:         []byte("random test string"),
		}
	}

	return ev
}

func TestScheduler_Schedule(t *testing.T) {
	cacheM, err := NewRedisCacheManager(RedisCacheManagerConfig{
		Addr: os.Getenv("REDIS_ADDR"),
		Pass: "",
		DB:   0,
	})

	if err != nil {
		t.Errorf("An error occurred while creating Redis cache manager: %s\n", err)
	}

	persM, err := NewSqlPersistenceManager(cacheM, SqlPersistenceManagerConfig{
		Url:    fmt.Sprintf("host=%s port=%s dbname=%s user=%s password='%s' sslmode=%s", os.Getenv("PG_ADDR"), "5432", "job_scheduler", "job_scheduler", "job_scheduler", "disable"),
		Driver: "postgres",
	})

	if err != nil {
		t.Errorf("An error occurred while creating SQL persistence manager: %s\n", err)
	}

	ev := generateScheduledEvents(10)

	count := 0
	cronJobCount := 0
	corruption := false

	sch := NewScheduler(context.Background(), SchedulerConfig{
		StackManagerConfig: StackManagerConfig{
			StacksNumber:         100,
			DefaultStackCapacity: 10,
			MaxStackCapacity:     15,
		},
		DispatchManagerConfig: DispatchManagerConfig{
			WorkerNumber:         100,
			DefaultQueueCapacity: 1000,
			MaxQueueCapacity:     1500,
		},
		DefaultInputQueueCapacity: 1200,
		MaxInputQueueCapacity:     1600,
		MaxBulkLimit:              2000,
	}, persM, cacheM, func(e Event) error {
		if e.Mode == CronMode {
			if cronJobCount == 3 {
				return nil
			}

			cronJobCount++

			return nil
		}

		count++

		if string(e.Payload) != string(ev[0].Payload) {
			corruption = true
		}

		return nil
	})

	if err := sch.Start(); err != nil {
		t.Errorf("An error occurred while starting the Scheduler: %s\n", err)
	}

	for _, e := range ev {
		if _, err := sch.Schedule(e); err != nil {
			t.Errorf("An error occurred while scheduling event: %s\n", err)
		}
	}

	start := time.Now()
	for count != 10 {
		if time.Now().Sub(start) > time.Minute*2 {
			break
		}
	}

	if count != 10 {
		t.Fatalf("Not every events got dispatched: expected: 10, got: %d\n", count)
	}

	if corruption {
		t.Fatalf("Events payload have been corrupted during the scheduling\n")
	}

	if _, err := sch.Schedule(Event{
		ID:              "",
		CronExpression:  "0/4 * * * * *",
		ShouldExecuteAt: time.Time{},
		Mode:            CronMode,
		Payload:         nil,
	}); err != nil {
		t.Errorf("An error occurred while scheduling event: %s\n", err)
	}

	time.Sleep(time.Second * 30)

	if cronJobCount != 3 {
		t.Fatalf("Not every events got dispatched: expected: 3, got: %d\n", cronJobCount)
	}
}
