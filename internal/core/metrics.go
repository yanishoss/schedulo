package core

import "time"

type metrics struct {
	ops       int64
	startTime time.Time
}

func newMetrics() metrics {
	return metrics{
		ops:       0,
		startTime: time.Now(),
	}
}

func (m *metrics) Op() {
	m.ops++
}

// In op/seconds
func (m *metrics) OpRate() float64 {
	delta := time.Now().Sub(m.startTime)

	return float64(m.ops) / delta.Seconds()
}
