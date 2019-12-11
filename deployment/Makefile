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

test:
	@echo Test ...
	@docker-compose exec custodian bash -c  "cd /go/src/ && ginkgo -r -keepGoing server"

.PHONY: default build run down test
