package core

import (
	"context"
)

type DispatchFunc func(Event) error

type DispatchManagerConfig struct {
	WorkerNumber         int
	DefaultQueueCapacity int
	MaxQueueCapacity     int
}

type dispatchManager interface {
	Dispatch(e event)
	Run()
	Stop()
}

type _dispatchManager struct {
	pers    PersistenceManager
	qu      eventQueue
	fn      DispatchFunc
	config  DispatchManagerConfig
	cancel  context.CancelFunc
	metrics *metrics
	ctx     context.Context
}

func newDispatchManager(ctx context.Context, pers PersistenceManager, fn DispatchFunc, config DispatchManagerConfig, metrics *metrics) dispatchManager {
	ctx, cancel := context.WithCancel(ctx)

	d := &_dispatchManager{
		pers:    pers,
		qu:      newEventQueue(config.DefaultQueueCapacity, config.MaxQueueCapacity),
		fn:      fn,
		config:  config,
		ctx:     ctx,
		cancel:  cancel,
		metrics: metrics,
	}

	return d
}

func (d *_dispatchManager) Run() {
	for i := 0; i < d.config.WorkerNumber; i++ {
		go d.run()
	}
}

func (d *_dispatchManager) Stop() {
	d.cancel()
}

func (d *_dispatchManager) Dispatch(e event) {
	d.qu.Lock()

	if err := d.qu.Push(e); err != nil {
		d.qu.Unlock()
		return
	}

	d.qu.Unlock()
}

func (d *_dispatchManager) dispatch(u event) {
	ev, err := d.pers.Get(d.ctx, u.ID)

	if err != nil {
		return
	}

	if ev.Mode == TimestampMode {
		if err := d.pers.Delete(d.ctx, ev.ID); err != nil {
			return
		}
	}

	if err := d.fn(ev); err != nil {
		return
	}

	d.metrics.Op()
}

func (d *_dispatchManager) run() {
	for {
		select {
		case <-d.ctx.Done():
			return
		default:
			d.qu.Lock()
			e := d.qu.Pop()
			d.qu.Unlock()

			if e == nil {
				break
			}

			d.dispatch(*e)
		}
	}
}
