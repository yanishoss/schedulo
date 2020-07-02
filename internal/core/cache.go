package core

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v7"
	"strconv"
	"time"
)

var (
	ErrNotImplemented = errors.New("this method is not implemented")
)

type CacheManager interface {
	PersistenceManager
}

type redisCacheManager struct {
	c *redis.Client
}

type RedisCacheManagerConfig struct {
	Addr string
	Pass string
	DB   int
}

func NewRedisCacheManager(conf RedisCacheManagerConfig) (CacheManager, error) {
	client := redis.NewClient(&redis.Options{
		Addr:            conf.Addr,
		Password:        conf.Pass,
		DB:              conf.DB,
		MaxRetries:      8,
		MinRetryBackoff: 0,
		MaxRetryBackoff: 30,
	})

	if err := client.Ping().Err(); err != nil {
		return nil, err
	}

	return &redisCacheManager{client}, nil
}

func (cache *redisCacheManager) Add(ctx context.Context, e Event) error {
	conn := cache.c.Conn()
	defer conn.Close()

	cmd := conn.HMSet(
		"schedulo_ns:"+string(e.ID),
		"cron_expression", e.CronExpression,
		"should_execute_at", e.ShouldExecuteAt.Format(time.RFC3339Nano),
		"mode", int(e.Mode),
		"topic", e.Topic,
		"payload", e.Payload,
	)

	if err := cmd.Err(); err != nil {
		return err
	}

	cmd = conn.Expire("schedulo_ns:"+string(e.ID), time.Minute*30)

	if err := cmd.Err(); err != nil {
		return err
	}

	return nil
}

func (cache *redisCacheManager) AddBulk(ctx context.Context, evs []Event) error {
	conn := cache.c.Conn()
	defer conn.Close()

	pip := conn.Pipeline()
	defer pip.Close()

	for _, e := range evs {
		cmd := pip.HMSet(
			"schedulo_ns:"+string(e.ID),
			"cron_expression", e.CronExpression,
			"should_execute_at", e.ShouldExecuteAt.Format(time.RFC3339Nano),
			"mode", int(e.Mode),
			"topic", e.Topic,
			"payload", e.Payload,
		)

		if err := cmd.Err(); err != nil {
			return err
		}

		cmd = pip.Expire("schedulo_ns:"+string(e.ID), time.Minute*30)

		if err := cmd.Err(); err != nil {
			return err
		}
	}

	_, err := pip.ExecContext(ctx)

	if err != nil {
		return err
	}

	return nil
}

func (cache *redisCacheManager) Get(ctx context.Context, id ID) (Event, error) {
	conn := cache.c.Conn()
	defer conn.Close()

	cmd := conn.Exists("schedulo_ns:"+string(id))

	if err := cmd.Err(); err != nil {
		return Event{}, err
	}

	if cmd.Val() == 0 {
		return Event{}, ErrNotFound
	}

	cmdGet := conn.HGetAll("schedulo_ns:"+string(id))

	if err := cmdGet.Err(); err != nil {
		return Event{}, err
	}

	e := Event{}

	obj := cmdGet.Val()

	e.ID = id
	e.CronExpression = obj["cron_expression"]
	var err error
	e.ShouldExecuteAt, err = time.Parse(time.RFC3339Nano, obj["should_execute_at"])

	if err != nil {
		return e, err
	}

	mode, err := strconv.Atoi(obj["mode"])

	if err != nil {
		return e, err
	}

	e.Mode = EventMode(mode)

	e.Topic = obj["topic"]

	e.Payload = []byte(obj["payload"])

	return e, nil
}

func (cache *redisCacheManager) Delete(ctx context.Context, id ID) error {
	conn := cache.c.Conn()
	defer conn.Close()

	cmd := conn.Del("schedulo_ns:"+string(id))

	if err := cmd.Err(); err != nil {
		return err
	}

	return nil
}

func (cache *redisCacheManager) GetAll(ctx context.Context) (out []Event, err error) {
	return out, ErrNotImplemented
}
