#!/bin/bash

RUST_LOG=debug cargo run -- \
    --destpulsar=pulsar+ssl://172.25.149.116:6651 \
    --desttopic=persistent://public/functions/clog_ola \
    --destsubscription=perf_aggregator \
    --desttrustcerts="../../zarf/certs/dev_copy/SIBS_PULSAR_ca-chain.bundle.cert.pem" \
    --destcertfile="../../zarf/certs/dev_copy/PULSAR_BROKER_CLIENT.cert.pem" \
    --destkeyfile="../../zarf/certs/dev_copy/PULSAR_BROKER_CLIENT.key.p8" \
    --hostname_verification_enabled \
    --allow_insecure_connection \
