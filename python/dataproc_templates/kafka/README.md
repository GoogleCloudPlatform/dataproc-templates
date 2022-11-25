# Kafka Metadata to GCS

Template for reading metadata from Kafka and Schema Registry and writing to Cloud Storage bucket.

## Arguments

* `kafkametadata.gcs.bootstrap.servers`: 
* `kafkametadata.gcs.api.key`: 
* `kafkametadata.gcs.api.secret`: 
* `kafkametadata.gcs.schemaregistry.endpoint`: 
* `kafkametadata.gcs.schemaregistry.api.key`: 
* `kafkametadata.gcs.schemaregistry.api.secret`: 
* `kafkametadata.gcs.output.location`: GCS location for output files (format: `gs://BUCKET/...`)
* `kafkametadata.gcs.output.format`: Output file format (one of: avro,parquet,csv,json) (Defaults to parquet)
* `kafkametadata.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to overwrite)

## Usage

```
$ python main.py --template KAFKAMETADATATOGCS -h

usage: main.py --template KAFKAMETADATATOGCS [-h] \
    --kafkametadata.gcs.bootstrap.servers KAFKAMETADATA.GCS.BOOTSTRAP.SERVERS \
    [--kafkametadata.gcs.api.key KAFKAMETADATA.GCS.API.KEY] \
    [--kafkametadata.gcs.api.secret KAFKAMETADATA.GCS.API.SECRET] \
    --kafkametadata.gcs.schemaregistry.endpoint KAFKAMETADATA.GCS.SCHEMAREGISTRY.ENDPOINT \
    [--kafkametadata.gcs.schemaregistry.api.key KAFKAMETADATA.GCS.SCHEMAREGISTRY.API.KEY] \
    [--kafkametadata.gcs.schemaregistry.api.secret KAFKAMETADATA.GCS.SCHEMAREGISTRY.API.SECRET] \
    --kafkametadata.gcs.output.location KAFKAMETADATA.GCS.OUTPUT.LOCATION \
    [--kafkametadata.gcs.output.format {avro,parquet,csv,json}] \
    [--kafkametadata.gcs.output.mode {overwrite,append,ignore,errorifexists}]

options:
  -h, --help            show this help message and exit
  --kafkametadata.gcs.bootstrap.servers KAFKAMETADATA.GCS.BOOTSTRAP.SERVERS
                        Kafka Bootstrap Servers list (host:port)
  --kafkametadata.gcs.api.key KAFKAMETADATA.GCS.API.KEY
                        Kafka API Key
  --kafkametadata.gcs.api.secret KAFKAMETADATA.GCS.API.SECRET
                        Kafka API Secret
  --kafkametadata.gcs.schemaregistry.endpoint KAFKAMETADATA.GCS.SCHEMAREGISTRY.ENDPOINT
                        Schema Registry Endpoint URL
  --kafkametadata.gcs.schemaregistry.api.key KAFKAMETADATA.GCS.SCHEMAREGISTRY.API.KEY
                        Schema Registry API Key
  --kafkametadata.gcs.schemaregistry.api.secret KAFKAMETADATA.GCS.SCHEMAREGISTRY.API.SECRET
                        Schema Registry API Secret
  --kafkametadata.gcs.output.location KAFKAMETADATA.GCS.OUTPUT.LOCATION
                        GCS location for output files
  --kafkametadata.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json) (Defaults to parquet)
  --kafkametadata.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to overwrite)
```

## Example submission

```
export GCP_PROJECT=MY_GCP_PROJECT_ID
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://my-bucket/temp
export SUBNET=default

./bin/start.sh \
    --template=KAFKAMETADATATOGCS \
    --kafkametadata.gcs.bootstrap.servers my-kafka-bootstrap-server
    --kafkametadata.gcs.api.key KAFKA_API_KEY
    --kafkametadata.gcs.api.secret KAFKA_API_SECRET
    --kafkametadata.gcs.schemaregistry.endpoint http://schemaregistry-endpoint
    --kafkametadata.gcs.schemaregistry.api.key SCHEMAREGISTRY_API_KEY
    --kafkametadata.gcs.schemaregistry.api.secret SCHEMAREGISTRY_API_SECRET
    --kafkametadata.gcs.output.location gs://my-bucket/kafkametadata/
    --kafkametadata.gcs.output.format parquet
    --kafkametadata.gcs.output.mode overwrite
```