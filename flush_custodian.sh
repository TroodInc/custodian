#!/bin/bash
/Applications/Postgres.app/Contents/Versions/10/bin/psql -p5432 -d "postgres" -c "drop database custodian"
/Applications/Postgres.app/Contents/Versions/10/bin/psql -p5432 -d "postgres" -c "create database custodian"
rm *.json || true