FROM golang:1.13

WORKDIR /go/src/app

RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && python /tmp/get-pip.py
RUN pip install requests

COPY ./migrate.py /usr/bin
COPY ./loaddata.py /usr/bin
RUN chmod +x /usr/bin/migrate.py
RUN chmod +x /usr/bin/loaddata.py

COPY ./src/logger /go/src/logger
COPY ./src/server /go/src/server
COPY ./src/utils /go/src/utils
COPY ./main.go /go/src/app
COPY ./docs/_static /home/static

RUN go get -d -v
RUN go install -v
RUN go get github.com/fatih/structs
RUN go get github.com/onsi/ginkgo/ginkgo
RUN go get github.com/onsi/gomega/...

WORKDIR /home/custodian

CMD app -p 8000
