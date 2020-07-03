#!/usr/bin/env bash

docker-compose down --remove-orphans || true
docker-compose build
docker-compose up --force-recreate --abort-on-container-exit --exit-code-from go_test && docker-compose down --remove-orphans