FROM golang:1.8.1

WORKDIR /go/src/app
COPY ./src/logger /go/src/logger
COPY ./src/server /go/src/server
COPY ./main.go .
COPY ./main.go .
COPY ./deployment/wait-for-postgres.sh .

RUN go get -d -v
RUN go install -v
EXPOSE 8000

CMD "./wait-for-postgres.sh"