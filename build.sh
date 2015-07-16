#!/bin/bash
mvn clean assembly:assembly

rm ./bin/couch-downloader 2> /dev/null # ignore if not there
cat ./bin/stub.sh ./target/couch-downloader-0.0.1-SNAPSHOT-jar-with-dependencies.jar > ./bin/couch-downloader
chmod +x ./bin/couch-downloader
