FROM golang:1.14

WORKDIR /go/src/app

RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && python /tmp/get-pip.py
RUN pip install requests

COPY ./migrate.py /usr/bin
COPY ./loaddata.py /usr/bin
RUN chmod +x /usr/bin/migrate.py
RUN chmod +x /usr/bin/loaddata.py

COPY ./src/logger /usr/local/go/src/logger
COPY ./src/server /usr/local/go/src/server
COPY ./src/utils /usr/local/go/src/utils
COPY ./main.go /go/src/app
COPY ./docs/_static /home/static
COPY ./VERSION /home/custodian/
COPY ./go.mod /go/src/app
COPY ./go.sum /go/src/app
COPY ./src/logger /go/src/app/logger
COPY ./src/server /go/src/app/server
COPY ./src/utils /go/src/app/utils

RUN echo $TAG > /home/custodian/VERSION

RUN go build -o app
RUN go get github.com/fatih/structs
RUN go get github.com/onsi/ginkgo/ginkgo
RUN go get github.com/onsi/gomega/...
EXPOSE 8080
CMD ["./app"]
