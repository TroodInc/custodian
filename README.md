Docker build example:

```bash
FROM golang:1.8.1

WORKDIR /go/src/app
COPY ./src/logger /go/src/logger
COPY ./src/server /go/src/server
COPY ./src/main.go .

RUN go get -d -v
RUN go install -v

CMD app -p 8000 -d "host=${POSTGRES_HOST} user=${POSTGRES_USERNAME} password=${POSTGRES_PASSWORD} dbname=custodian sslmode=disable"
```


