#!/usr/bin/env bash

docker restart $(docker ps --format "{{.ID}}" --filter "name=postgres_skunk")