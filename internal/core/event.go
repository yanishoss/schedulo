package core

import "time"

const (
	TimestampMode = iota
	CronMode
)

type EventMode uint

type ID string

type event struct {
	ID              ID
	CronExpression  string
	ShouldExecuteAt time.Time
	Mode            EventMode
}

type Event struct {
	ID ID

	// CronExpression allows us to schedule the event based on the cron expression
	CronExpression string

	// ShouldExecuteAt is the timestamp at which the event must be scheduled
	ShouldExecuteAt time.Time

	// Mode is the mode in which the event should be scheduled (CronMode or TimestampMode)
	Mode EventMode

	// Topic is the channel on which the event has to be dispatched
	Topic string

	// Payload is the content of the event
	Payload []byte
}
