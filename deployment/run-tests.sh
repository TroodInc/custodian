#!/bin/bash -e
# wait-for-postgres.sh
# Adapted from https://docs.docker.com/compose/startup-order/

# Expects the necessary PG* variables.

./wait-for-it.sh postgres.custodian:5432 -- ginkgo -r logger server utils