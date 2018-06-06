FROM golang:1.10

WORKDIR /go/src/app
COPY ./src/logger /go/src/logger
COPY ./src/server /go/src/server
COPY ./src/utils /go/src/utils
COPY ./main.go .
COPY ./deployment/wait-for-postgres.sh .

RUN go get -d -v
RUN go install -v
EXPOSE 8000

CMD "./wait-for-postgres.sh"