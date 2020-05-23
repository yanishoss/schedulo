package schedulo

import (
	"context"
	"github.com/yanishoss/schedulo/api"
	"github.com/yanishoss/schedulo/internal/core"
	"google.golang.org/grpc"
	"io"
	"time"
)

const (
	TimestampMode = core.TimestampMode
	CronMode = core.TimestampMode
)

type Event = core.Event
type ID = core.ID

type Client interface {
	Schedule(ctx context.Context, e Event) (ID, error)
	Unschedule(ctx context.Context, id ID) error
	OnEvent(ctx context.Context, topic string, cb func(Event), cbErr func(error)) (func(), error)
	ListenToEvent(ctx context.Context, topic string, cb func(Event)) error
	Close() error
}

type client struct {
	c api.SchedulerClient
	conn *grpc.ClientConn
}

func New(addr string) (Client, error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*20)
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())

	if err != nil {
		return nil, err
	}

	c := api.NewSchedulerClient(conn)

	return &client{c, conn}, nil
}

func (cl *client) Schedule(ctx context.Context, e core.Event) (core.ID, error) {
	ctx, _ = context.WithTimeout(ctx, 20*time.Second)

	ev := coreEventToApiEvent(e)

	resp, err := cl.c.Schedule(ctx, &api.ScheduleRequest{Event: &ev})

	if err != nil {
		return "", err
	}

	return core.ID(resp.Id.Id), nil
}

func (cl *client) Unschedule(ctx context.Context, id core.ID) error {
	ctx, _ = context.WithTimeout(ctx, 20*time.Second)

	_, err := cl.c.Unschedule(ctx, &api.UnscheduleRequest{
		Id: &api.Event_ID{
			Id: string(id),
		},
	})

	return err
}

func (cl *client) Close() error {
	return cl.conn.Close()
}

func (cl *client) OnEvent(ctx context.Context, topic string, cb func(core.Event), cbErr func(error)) (func(), error) {
	stream, err := cl.c.StreamEvents(ctx, &api.StreamEventsRequest{
		Topic: topic,
	})

	if err != nil {
		return nil, err
	}

	done := make(chan bool)

	close_ := func() {
		done <- true
	}

	go func() {
		for {
			select {
			case <- done:
				if err := stream.CloseSend(); err != nil {
					cbErr(err)
				}

				return
			case <- stream.Context().Done():
				cbErr(stream.Context().Err())
				return
			default:
				resp, err := stream.Recv()

				if err == io.EOF {
					cbErr(err)
					return
				}

				if err != nil {
					cbErr(err)
					break
				}

				cb(apiEventToCoreEvent(*resp.Event))
			}
		}
	}()

	return close_, nil
}

func (cl *client) ListenToEvent(ctx context.Context, topic string, cb func(core.Event)) error {
	stream, err := cl.c.StreamEvents(ctx, &api.StreamEventsRequest{
		Topic: topic,
	})

	if err != nil {
		return err
	}

	for {
		select {
		case <- stream.Context().Done():
			return stream.Context().Err()
		default:
			resp, err := stream.Recv()

			if err == io.EOF {
				return nil
			}

			if err != nil {
				return err
			}

			cb(apiEventToCoreEvent(*resp.Event))
		}
	}
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