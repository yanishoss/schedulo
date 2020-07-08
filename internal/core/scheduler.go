package core

import (
	"context"
	"github.com/robfig/cron/v3"
	circuit "github.com/rubyist/circuitbreaker"
	uuid "github.com/satori/go.uuid"
	"math"
	"time"
)

type Scheduler interface {
	Schedule(e Event) (ID, error)
	Unschedule(id ID) error
	schedule(e event)
	Start() error
	Stop()
	SetConfig(conf SchedulerConfig) error
}

type scheduler struct {
	dpM           dispatchManager
	pM            PersistenceManager
	cM            CacheManager
	sM            stackManager
	dpFn          DispatchFunc
	ctx           context.Context
	cancel        context.CancelFunc
	cr            cron.Parser
	inputMetrics  *metrics
	outputMetrics *metrics
	workers       []processingWorker
	queue         rawEventQueue
	conf          SchedulerConfig
}

type SchedulerConfig struct {
	StackManagerConfig
	DispatchManagerConfig
	DefaultInputQueueCapacity int
	MaxInputQueueCapacity     int
	MaxBulkLimit              int
}

func NewScheduler(ctx context.Context, conf SchedulerConfig, pers PersistenceManager, cache CacheManager, fn DispatchFunc) Scheduler {
	ctx, cancel := context.WithCancel(ctx)

	inputMet := newMetrics()
	outputMet := newMetrics()

	sM := newStackManager(conf.StackManagerConfig)
	dpM := newDispatchManager(ctx, pers, fn, conf.DispatchManagerConfig, &outputMet)

	sch := &scheduler{
		dpM:           dpM,
		pM:            pers,
		cM:            cache,
		cancel:        cancel,
		sM:            *sM,
		conf:          conf,
		ctx:           ctx,
		dpFn:          fn,
		cr:            cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		workers:       make([]processingWorker, conf.StacksNumber),
		queue:         newRawEventQueue(conf.DefaultInputQueueCapacity, conf.MaxInputQueueCapacity),
		inputMetrics:  &inputMet,
		outputMetrics: &outputMet,
	}

	for i := 0; i < conf.StackManagerConfig.StacksNumber; i++ {
		sch.workers[i] = newProcessingWorker(ctx, sM.stacks[i], sch, dpM)
	}

	return sch
}

func (sch *scheduler) Unschedule(id ID) error {
	return sch.pM.Delete(sch.ctx, id)
}

func (sch *scheduler) Schedule(e Event) (ID, error) {
	sch.inputMetrics.Op()

	if e.ShouldExecuteAt.Before(time.Now()) {
		e.ShouldExecuteAt = time.Now()
	}

	if e.Mode == CronMode {
		s, err := sch.cr.Parse(e.CronExpression)

		if err != nil {
			return "", err
		}

		e.ShouldExecuteAt = s.Next(e.ShouldExecuteAt)
	}

	id := uuid.NewV4().String()
	e.ID = ID(id)

	sch.queue.Lock()
	defer sch.queue.Unlock()

	return e.ID, sch.queue.Push(e)
}

func (sch *scheduler) schedule(e event) {
	_, err := sch.pM.Get(sch.ctx, e.ID)

	if err == ErrNotFound {
		return
	}

	if e.Mode == CronMode {
		s, err := sch.cr.Parse(e.CronExpression)

		if err != nil {
			return
		}

		e.ShouldExecuteAt = s.Next(e.ShouldExecuteAt)
	}

	if err := sch.sM.Push(e); err != nil {
		return
	}
}

func (sch *scheduler) run() {
	fn := func() {
		sch.queue.Lock()
		if sch.queue.len == 0 {
			sch.queue.Unlock()
			return
		}
		sch.queue.Unlock()

		rate := sch.inputMetrics.OpRate()

		limit := int(math.Ceil(rate * 5))

		if limit > sch.conf.MaxBulkLimit {
			limit = sch.conf.MaxBulkLimit
		}

		evs := make([]Event, 0, limit)

		sch.queue.Lock()
		for i := 0; i < limit; i++ {
			e := sch.queue.Pop()

			if e == nil {
				break
			}

			evs = append(evs, *e)
		}
		sch.queue.Unlock()

		cb := circuit.NewThresholdBreaker(12)

		err := cb.CallContext(sch.ctx, func() error {return sch.pM.AddBulk(sch.ctx, evs)}, 0)

		if err != nil {
			return
		}

		err = cb.CallContext(sch.ctx, func() error {return sch.cM.AddBulk(sch.ctx, evs)}, 0)

		if err != nil {
			return
		}

		for _, e := range evs {
			err := sch.sM.Push(event{
				ID:              e.ID,
				CronExpression:  e.CronExpression,
				ShouldExecuteAt: e.ShouldExecuteAt,
				Mode:            e.Mode,
			})

			if err != nil {
				return
			}
		}
	}

	for {
		select {
		case <-sch.ctx.Done():
			return
		default:
			fn()
		}
	}
}

func (sch *scheduler) restoreEventsAtStartup() error {
	evs, err := sch.pM.GetAll(sch.ctx)

	if err != nil {
		return err
	}

	for _, e := range evs {
		if err := sch.sM.Push(event{
			ID:              e.ID,
			CronExpression:  e.CronExpression,
			ShouldExecuteAt: e.ShouldExecuteAt,
			Mode:            e.Mode,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (sch *scheduler) Start() error {
	sch.dpM.Run()

	for _, p := range sch.workers {
		p.Run()
	}

	go sch.run()

	if err := sch.restoreEventsAtStartup(); err != nil {
		return err
	}

	return nil
}

func (sch *scheduler) Stop() {
	sch.dpM.Stop()

	for _, p := range sch.workers {
		p.Stop()
	}

	sch.cancel()
}

func (sch *scheduler) SetConfig(conf SchedulerConfig) error {
	err := sch.sM.SetConfig(conf.StackManagerConfig)

	if err != nil {
		return err
	}

	sch.dpM.Stop()
	sch.dpM = newDispatchManager(sch.ctx, sch.pM, sch.dpFn, conf.DispatchManagerConfig, sch.outputMetrics)
	defer sch.dpM.Run()

	delta := len(sch.workers) - conf.StacksNumber

	if delta > 0 {
		toDelete := sch.workers[:delta]

		for _, p := range toDelete {
			p.Stop()
		}

		sch.workers = sch.workers[delta:]
	}

	if delta < 0 {
		oldLen := len(sch.workers)

		for i := oldLen; i < oldLen+(-delta); i++ {
			p := newProcessingWorker(sch.ctx, sch.sM.stacks[i], sch, sch.dpM)

			sch.workers = append(sch.workers, p)

			p.Run()
		}
	}

	return nil
}
