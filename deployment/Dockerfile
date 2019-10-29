FROM golang:1.10

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
COPY ./deployment/wait-for-it.sh /go/bin
COPY ./main.go /go/src/app
COPY ./docs/_static /home/static

RUN go get -d -v
RUN go install -v
RUN go get github.com/fatih/structs
RUN go get github.com/onsi/ginkgo/ginkgo
RUN go get github.com/onsi/gomega/...

RUN chmod +x /go/bin/wait-for-it.sh

WORKDIR /home/custodian

ARG DB_HOST
ARG DB_USER
ARG DB_PASSWORD
ARG DB_NAME
ARG DB_PORT
ARG DB_SSL_MODE

ENV DB_CONNECTION_OPTIONS "host=$DB_HOST user=$DB_USER password=$DB_PASSWORD port=$DB_PORT dbname=$DB_NAME sslmode=$DB_SSL_MODE"
ENV DB_HOST ${DB_HOST}
ENV DB_PORT ${DB_PORT}

EXPOSE 8000
CMD /go/bin/wait-for-it.sh $DB_HOST:$DB_PORT -- app -p 8000 -d "$DB_CONNECTION_OPTIONS" --auth "$AUTH_URL"
