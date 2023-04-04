#!/bin/bash

RUST_LOG=debug cargo run -- \
    --sourcepulsar=pulsar+ssl://172.25.149.116:6651 \
    --sourcetopic=persistent://public/functions/agg_ola \
    --sourcesubscription=perf_aggregator \
    --sourcetrustcerts="../../zarf/certs/dev_copy/SIBS_PULSAR_ca-chain.bundle.cert.pem" \
    --sourcecertfile="../../zarf/certs/dev_copy/PULSAR_BROKER_CLIENT.cert.pem" \
    --sourcekeyfile="../../zarf/certs/dev_copy/PULSAR_BROKER_CLIENT.key.p8" \
    --hostname_verification_enabled \
    --allow_insecure_connection \
