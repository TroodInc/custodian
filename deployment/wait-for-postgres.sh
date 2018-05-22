#!/bin/bash -e
# wait-for-postgres.sh
# Adapted from https://docs.docker.com/compose/startup-order/

# Expects the necessary PG* variables.

until $(app -p 8000 -d "host=custodian_postgres user=custodian password=custodian dbname=custodian sslmode=disable" --auth "http://authorization.trood:8000"); do
  echo >&2 "Postgres is unavailable - sleeping"
  sleep 1
done