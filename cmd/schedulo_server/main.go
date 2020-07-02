package main

import (
	"context"
	"flag"
	"fmt"
	circuit "github.com/rubyist/circuitbreaker"
	"github.com/yanishoss/schedulo/api"
	"github.com/yanishoss/schedulo/cmd/schedulo_server/config"
	"github.com/yanishoss/schedulo/cmd/schedulo_server/server"
	"github.com/yanishoss/schedulo/internal/core"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

func main() {
	home, err := os.UserHomeDir()

	if err != nil {
		log.Fatalln(err)
		return
	}

	confPath := flag.String("path-to-config", home+"/schedulo.config.yaml", "path to the Schedulo configuration file")
	port := flag.Int("port", 9876, "port is the tcp port on which the server will be listening")
	addr := flag.String("addr", "0.0.0.0", "addr is the tcp address the server will be listening on")
	flag.Parse()

	cfg := config.GetConfig(*confPath)

	if *port != 9876 {
		cfg.Network.Port = *port
	}

	if *addr != "0.0.0.0" {
		cfg.Network.Addr = *addr
	}

	cb := circuit.NewBreakerWithOptions(&circuit.Options{ShouldTrip: func(b *circuit.Breaker) bool {
		return false
	}})

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Network.Addr, cfg.Network.Port))

	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}

	ctx := context.Background()

	grpcServer := grpc.NewServer()

	var cache core.CacheManager

	err = cb.Call(func() error {
		cache, err = core.NewRedisCacheManager(core.RedisCacheManagerConfig{
			Addr: cfg.Cache.Addr,
			Pass: cfg.Cache.Pass,
			DB:   cfg.Cache.DB,
		})

		return err
	}, 0)

	if err != nil {
		log.Fatalf("failed to initialize Redis: %v\n", err)
	}

	var pers core.PersistenceManager

	err = cb.Call(func() error {
		pers, err = core.NewSqlPersistenceManager(cache, core.SqlPersistenceManagerConfig{
			Url:    cfg.Database.Url,
			Driver: cfg.Database.Driver,
		})

		return err
	}, 0)

	if err != nil {
		log.Fatalf("failed to initialize SQL: %v\n", err)
	}

	srv, err := server.New(ctx, core.SchedulerConfig{
		StackManagerConfig: core.StackManagerConfig{
			StacksNumber:         cfg.System.StacksNumber,
			DefaultStackCapacity: cfg.System.DefaultStackCapacity,
			MaxStackCapacity:     cfg.System.MaxStackCapacity,
		},
		DispatchManagerConfig: core.DispatchManagerConfig{
			WorkerNumber:         cfg.Dispatch.WorkersNumber,
			DefaultQueueCapacity: cfg.Dispatch.DefaultQueueCapacity,
			MaxQueueCapacity:     cfg.Dispatch.MaxQueueCapacity,
		},
		DefaultInputQueueCapacity: cfg.Input.DefaultQueueCapacity,
		MaxInputQueueCapacity:     cfg.Input.MaxQueueCapacity,
		MaxBulkLimit:              cfg.Input.MaxBulkLimit,
	}, pers, cache)

	if err != nil {
		log.Fatalf("failed to initialize server: %v\n", err)
	}

	api.RegisterSchedulerServer(grpcServer, srv)

	log.Printf("listening to tcp://%s:%d\n", cfg.Network.Addr, cfg.Network.Port)

	log.Fatalf("failed to serve: %v\n", grpcServer.Serve(lis))
}
