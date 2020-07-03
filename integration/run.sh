#!/usr/bin/env bash

docker-compose down --remove-orphans || true
docker-compose build
docker-compose up --detach --force-recreate

cd ../
GOMOD111=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test --cover -v github.com/yanishoss/schedulo/cmd/schedulo_server && cd integration/ && docker-compose down --remove-orphans
