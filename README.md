# Couch-downloader

A simple tool written in Java to download documents from Couchbase.

# Building

Build standalone jar

```
./build.sh
```

# Before Running

This utility requires that a special design document and view be created that would return all (or any particular) documents in your bucket you want to download.

Login to your Couchbase Web Admin UI. Create a new design document named `CouchDownloader`.  Add a new view named `all_docs` under this design document.  Publish the view.

# Running

Edit the `run.sh` script with appropriate parameters and run it or run directly:

```
> cd bin
> ./couch-downloader
usage: couch-downloader -[OPTION] [value]
 -b <arg>    (Required) Couch bucket name.
 -download   (Optional) Download mode. Default: false
 -h <arg>    (Required) Couchbase host name. Port is assumed to be 8091.
 -o <arg>    (Optional) Output directory path. Default: /tmp/couchbase
 -p <arg>    (Required) Couchbase password.
 -upload     (Optional) Upload mode. Default: false

> ./couch-downloader -b MY_BUCKET -h MY_COUCHBASE_HOST -o "/tmp/couchbase/`date +%s`" -p MY_BUCKET_PASSWORD -download
```

Enjoy. :shipit: