FROM golang:1.19 as builder

WORKDIR /go/src/app

COPY . .

RUN go build -o /go/app

# COPY ./logger /go/src/app/logger
# COPY ./server /go/src/app/server
# COPY ./utils /go/src/app/utils
FROM golang:1.19
COPY --from=builder /go/app /go/app
COPY ./migrate.py  ./loaddata.py /usr/bin/
COPY ./docs/_static /home/static
COPY ./VERSION /home/custodian/

#RUN chmod +x /usr/bin/migrate.py \
#    && chmod +x /usr/bin/loaddata.py \
#    && apt-get update \
#    && apt-get install -y python3-distutils \
#    && curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
#    && python3 /tmp/get-pip.py --break-system-packages \
#    && pip install requests --break-system-packages \
#    && pip install trood==1.0.0 --break-system-packages \
#    && echo $TAG > /home/custodian/VERSION

WORKDIR /home/custodian
EXPOSE 8000
CMD /go/app
