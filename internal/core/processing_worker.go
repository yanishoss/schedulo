package core

import (
	"context"
	"runtime"
	"time"
)

type stopChan chan bool

type processingWorker interface {
	Run()
	Stop()
}

type _processingWorker struct {
	stackM   *stackManager
	stack    *stack
	dispatch dispatchManager
	sch      Scheduler
	stop     stopChan
	ctx      context.Context
	cancel   context.CancelFunc
}

func newProcessingWorker(ctx context.Context, stack *stack, sch Scheduler, dispatch dispatchManager) processingWorker {
	ctx, cancel := context.WithCancel(ctx)
	return &_processingWorker{
		sch:      sch,
		stack:    stack,
		dispatch: dispatch,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (p *_processingWorker) Run() {
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			default:
				p.process()
				runtime.Gosched()
			}
		}
	}()
}

func (p *_processingWorker) Stop() {
	p.cancel()
}

func (p *_processingWorker) process() {
	p.stack.Lock()
	defer p.stack.Unlock()

	if p.stack.len == 0 {
		return
	}

	e := p.stack.Get(0)

	now := time.Now()

	if e.ShouldExecuteAt.Before(now) || e.ShouldExecuteAt.Equal(now) {
		p.stack.Pop()

		p.dispatch.Dispatch(*e.event)

		if e.Mode == CronMode {
			p.sch.schedule(*e.event)
		}
	}
}
