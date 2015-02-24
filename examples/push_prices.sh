#!/bin/bash
../importer -writes=5000 \
           -table=prices \
           -mapping=GTINStore,Current
           -test \
           sample_prices.data
