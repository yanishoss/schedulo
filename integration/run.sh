#!/usr/bin/env bash

docker-compose down --remove-orphans || true
docker-compose build
docker-compose up --detach --force-recreate

cd ../
until nc localhost 9876
do
  echo ...
  sleep 1
done

GOMOD111=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 SCHEDULO_ADDR=localhost:9876 go test --cover -v github.com/yanishoss/schedulo/cmd/schedulo_server && cd integration/ && docker-compose down --remove-orphans
