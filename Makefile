PROJECT_NAME ?= custodian
DOCKER := $(shell which docker)
GOPATH := $(GOPATH):$(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
GOBIN := $(GOBASE)/bin
GLIDE := $(shell which glide)
GCLOUD := $(shell which gcloud)
COMMIT = $(strip $(shell git rev-parse --short HEAD))
BRANCH = $(strip $(shell git rev-parse --abbrev-ref HEAD))
GOLDFLAGS = "-w -X main.GitCommit=$(COMMIT) -X main.GitBranch=$(BRANCH)"
TAG ?= latest

default: build

build:
	@docker build -t $(PROJECT_NAME):$(TAG) -f deployment/Dockerfile .

deps:
ifeq (, $(DOCKER))
	$(warning "Docker is not installed. Try to run this: curl https://get.docker.com | sh")
endif
ifeq (, $(GLIDE))
	$(warning "Glide is not installed. Try to run this: curl https://glide.sh/get | sh")
endif
ifeq (, $(GCLOUD))
	$(error "Can't find Google Cloud SDK :(")
endif

install:
	@echo -n "Checking if there is any missing dependencies ... "
	@$(GLIDE) install
	@echo DONE

compile: install
	@echo -n "Compile $(PROJECT_NAME) binary ... "
	@echo GOPATH= $$GOPATH
	@cd ./src \
        && GOPATH=$(GOPATH) GOBIN=$(GOBIN) go build -ldflags $(GOLDFLAGS) -o $(PROJECT_NAME) \
		&& mv $(PROJECT_NAME) ../../ \
		&& chmod +x ../../$(PROJECT_NAME)
	@echo DONE

clean:
	@echo -n "Cleaning build cache ... "
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) go clean
	@echo OK

up:
	@cd deployment/ && docker-compose up

down:
	@cd deployment/ && docker-compose down


flush:
	@echo -n "Flushing database and migrations ... "
	@rm -f *.json
	@export $$(cat .env | grep -v ^\# | xargs) && \
		export PGHOST=$$DB_HOST PGDATABASE=$$DB_NAME PGUSER=$$DB_USER \
				PGPASSWORD=$$DB_PASSWORD PGSSLMODE=$$DB_SSL_MODE && \
		psql -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; \
				GRANT ALL ON SCHEMA public TO $$DB_USER; \
				GRANT ALL ON SCHEMA public TO public;"
	@echo DONE

gcloud-install:
	@gcloud components install kubectl

.PHONY: default build install clean up down deps flush flushdb gcloud-install compile
