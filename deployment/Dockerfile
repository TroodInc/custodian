FROM golang:1.10

WORKDIR /go/src/app

COPY ./src/logger /go/src/logger
COPY ./src/server /go/src/server
COPY ./src/utils /go/src/utils
COPY ./main.go /go/src/app
COPY ./deployment/wait-for-postgres.sh /go/bin

RUN go get -d -v
RUN go install -v

WORKDIR /home/custodian

EXPOSE 8000

CMD "/go/bin/wait-for-postgres.sh"