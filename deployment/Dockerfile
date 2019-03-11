FROM golang:alpine

# Build args
ARG BUILD_DATE
ARG VCS_REF
ARG VCS_BRANCH

ARG DB_HOST
ARG DB_USER
ARG DB_PASSWORD
ARG DB_NAME
ARG DB_PORT
ARG DB_SSL_MODE

LABEL org.label-schema.name="custodian" \
    org.label-schema.build-date=$BUILD_DATE \
    org.label-schema.usage="README.md" \
    org.label-schema.vendor="Trood" \
    org.label-schema.vcs-ref=$VCS_REF \
    org.label-schema.docker.cmd="docker run --rm --publish 8000:8000 --name custodian custodian:latest" \
    org.label-schema.schema-version="1.0"

# Environment
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV DB_CONNECTION_OPTIONS "host=$DB_HOST user=$DB_USER password=$DB_PASSWORD port=$DB_PORT dbname=$DB_NAME sslmode=$DB_SSL_MODE"
ENV DB_HOST ${DB_HOST}
ENV DB_PORT ${DB_PORT}

RUN echo DB_CONNECTION_OPTIONS= ${DB_CONNECTION_OPTIONS}

# Install requirements
RUN apk add --no-cache python3 curl git ca-certificates make
RUN curl https://glide.sh/get | sh
RUN python3 -m ensurepip &&  \
    pip3 install pipenv && \
    pipenv install && \
    mkdir /app

# Install scripts
COPY ./migrate.py /usr/bin
RUN chmod +x /usr/bin/migrate.py

COPY . /app
WORKDIR /app

# XXX: Dirty as hell
RUN cp -R src/logger src/utils src/server /go/src

# Compile binary
RUN make compile

VOLUME /data
EXPOSE 8000

CMD custodian -p 8000 -d "$DB_CONNECTION_OPTIONS" --auth "$AUTH_URL"
