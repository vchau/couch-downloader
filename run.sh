#!/bin/bash

./bin/couch-downloader -b MY_BUCKET_NAME -h MY_COUCHBASE_HOST -o "/tmp/couchbase/`date +%s`" -p MY_BUCKET_PASSWORD -download