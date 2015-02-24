#!/bin/bash
../importer -writes=5000 \
           -table=gtins \
           -mapping=GTIN,SKU,IsPrimaryUPC \
           -test \
           sample_gtins.data
