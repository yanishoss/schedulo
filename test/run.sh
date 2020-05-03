#!/usr/bin/env bash

docker-compose build
docker-compose up --abort-on-container-exit --force-recreate --exit-code-from go_test
docker-compose down --remove-orphans