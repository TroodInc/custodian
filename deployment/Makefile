P ?= server

default: run

build:
	@echo Build ...
	@docker-compose build

run:
	@echo Run ...
	@docker-compose up -d

down:
	@echo Down ...
	@docker-compose down

cleanup:
	find ../server -name "*.json" -delete
	@docker-compose exec custodian_postgres psql -U custodian -d custodian -c "drop schema public cascade; create schema public;"

test-all:
	@make cleanup
	@echo Testing all ...
	@docker-compose exec custodian bash -c  "cd /go/src/app/ && ginkgo -r -keepGoing $(P)"

test-fast:
	@make cleanup
	@docker-compose exec custodian bash -c  "cd /go/src/app/ && ginkgo --v --failFast --progress -r $(P)"

.PHONY: default build run down test
