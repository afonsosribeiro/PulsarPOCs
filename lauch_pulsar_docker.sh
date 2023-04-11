#!/bin/bash

docker run -d --rm --name pulsar \
    -p 6650:6650 \
    -p 6651:6651 \
    -p 8080:8080 \
    -p 8081:8081 \
    apachepulsar/pulsar:2.9.3 \
    bin/pulsar standalone
