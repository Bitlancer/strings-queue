#!/bin/bash

REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt
export REQUESTS_CA_BUNDLE

python -m jobqueue.queue_processor $@
