FROM golang:1.14

WORKDIR /go/src/app

COPY ./migrate.py /usr/bin
COPY ./loaddata.py /usr/bin

COPY ./main.go /go/src/app
COPY ./docs/_static /home/static
COPY ./VERSION /home/custodian/
COPY ./go.mod /go/src/app
COPY ./logger /go/src/app/logger
COPY ./server /go/src/app/server
COPY ./utils /go/src/app/utils

RUN chmod +x /usr/bin/migrate.py \
    && chmod +x /usr/bin/loaddata.py \
    && apt-get update \
    && apt-get install -y python3-distutils \
    && curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
    && python3 /tmp/get-pip.py \
    && pip install requests \
    && pip install trood==1.0.0 \
    && echo $TAG > /home/custodian/VERSION \
    && go build -o /go/app \
    && go get github.com/onsi/ginkgo/ginkgo \
    && go get github.com/onsi/gomega/...

WORKDIR /home/custodian
EXPOSE 8000
CMD /go/app
