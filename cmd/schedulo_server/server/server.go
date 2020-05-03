package server

import (
	"context"
	"errors"
	"github.com/yanishoss/schedulo/api"
	"github.com/yanishoss/schedulo/internal/core"
	"io"
	"time"
)

var (
	ErrUnknownTopic = errors.New("this topic is unknown")
)

type listener struct {
	core.DispatchFunc
	id int64
}
type listenerMap map[string][]listener

type Server struct {
	scheduler core.Scheduler
	listeners listenerMap
}

func New(ctx context.Context, config core.SchedulerConfig, pers core.PersistenceManager, cache core.CacheManager) (api.SchedulerServer, error) {
	s := &Server{listeners: make(listenerMap)}

	sch := core.NewScheduler(ctx, config, pers, cache, s.onDispatch)

	s.scheduler = sch

	if err := sch.Start(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) onDispatch(e core.Event) error {
	if e.Topic == "" {
		var err error

		for _, v := range s.listeners {
			for _, l := range v {
				if errL := l.DispatchFunc(e); errL != nil {
					err = errL
				}
			}
		}

		return err
	}

	topic, ok := s.listeners[e.Topic]

	if !ok {
		return ErrUnknownTopic
	}

	var err error

	for _, l := range topic {
		if errL := l.DispatchFunc(e); errL != nil {
			err = errL
		}
	}

	return err
}

func (s *Server) registerListener(topic string, l core.DispatchFunc) int64 {
	if _, ok := s.listeners[topic]; !ok {
		s.listeners[topic] = make([]listener, 0, 10)
	}

	var id int64 = 0

	if ln := len(s.listeners[topic]); ln > 0 {
		id = s.listeners[topic][ln-1].id + 1
	}

	s.listeners[topic] = append(s.listeners[topic], listener{l, id})

	return id
}

func (s *Server) unregisterListener(topic string, id int64) {
	for i, lis := range s.listeners[topic] {
		if lis.id == id {
			s.listeners[topic] = append(s.listeners[topic][:i], s.listeners[topic][i+1:]...)
		}
	}
}

func (s *Server) Schedule(ctx context.Context, req *api.ScheduleRequest) (*api.ScheduleResponse, error) {
	e := apiEventToCoreEvent(*req.Event)

	id, err := s.scheduler.Schedule(e)

	if err != nil {
		return &api.ScheduleResponse{}, err
	}

	return &api.ScheduleResponse{
		Id: &api.Event_ID{
			Id: string(id),
		},
	}, nil
}

func (s *Server) Unschedule(ctx context.Context, req *api.UnscheduleRequest) (*api.UnscheduleResponse, error) {
	return &api.UnscheduleResponse{}, s.scheduler.Unschedule(core.ID(req.Id.Id))
}

func (s *Server) StreamEvents(req *api.StreamEventsRequest, stream api.Scheduler_StreamEventsServer) error {
	var id int64

	d := func(e core.Event) error {
		resp := coreEventToApiEvent(e)

		err := stream.Send(&api.StreamEventsResponse{Event: &resp})

		if err == io.EOF {
			s.unregisterListener(req.Topic, id)
		}

		return err
	}

	id = s.registerListener(req.Topic, d)

	<-stream.Context().Done()
	err := stream.Context().Err()

	s.unregisterListener(req.Topic, id)
	return err
}

func coreEventToApiEvent(e core.Event) api.Event {
	var mode api.Event_Mode

	if e.Mode == core.TimestampMode {
		mode = api.Event_TIMESTAMP
	} else {
		mode = api.Event_CRON
	}

	return api.Event{
		Id:              string(e.ID),
		CronExpression:  e.CronExpression,
		ShouldExecuteAt: e.ShouldExecuteAt.Unix(),
		Mode:            mode,
		Topic:           e.Topic,
		Payload:         e.Payload,
	}
}

func apiEventToCoreEvent(e api.Event) core.Event {
	var mode core.EventMode

	if e.Mode == api.Event_TIMESTAMP {
		mode = core.TimestampMode
	} else {
		mode = core.CronMode
	}

	return core.Event{
		ID:              core.ID(e.Id),
		CronExpression:  e.CronExpression,
		ShouldExecuteAt: time.Unix(e.ShouldExecuteAt, 0),
		Mode:            mode,
		Topic:           e.Topic,
		Payload:         e.Payload,
	}
}
