#!/usr/bin/env bash

POSTGRES_VERSION=13.2-alpine  # see https://hub.docker.com/_/postgres
CONTAINER_NAME=postgres_skunk # Name of the docker container
EXPOSED_PORT=11312            # this is the port on the host machine
INTERNAL_PORT=5432            # this is the default port on which postgresql starts on within the container.
MAX_CONNECTIONS=115           # default is 115

DB_NAME=skunk_broken_pipe
DB_USER=skunk-broken-pipe
DB_PASS=skunk-broken-pipe

if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
  if [ ! "$(docker ps -aq -f name=$CONTAINER_NAME -f status=exited)" ]; then
    echo "Stopping postgres container"
    docker stop $CONTAINER_NAME
  fi
  echo "Starting postgres container"
  docker start $CONTAINER_NAME
else
  echo "Creating & starting postgres container"
  docker run -d \
    --name $CONTAINER_NAME \
    -p $EXPOSED_PORT:$INTERNAL_PORT \
    -e POSTGRES_DB=$DB_NAME \
    -e POSTGRES_USER=$DB_USER \
    -e POSTGRES_PASSWORD=$DB_PASS \
    postgres:$POSTGRES_VERSION \
    postgres -N $MAX_CONNECTIONS
fi
