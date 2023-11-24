# General Template

The general template is configured via a yaml file. For this file to be accessible, it must be
provided on startup using the spark submit argument `--files`. The template must be provided the
file name with the template argument `--config`.

## Configuration

This job requires a yaml file as config. The yaml is converted into
the [GeneralTemplateConfig](../config/GeneralTemplateConfig.java) pojo.

The configuration consists of 3 maps:

1. Input - A map of keys to settings to
   create [DataFrameReader](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html)
   objects with the `format`, `options` and `path`. These are passed directly to the DataFrameReader
   so any spark supported formats and options are paths are available. Temporary views will be
   created for each input and made available for queries and will be named by the keys.
2. Query - A map of keys to Spark SQL queries to execute on the input. The output of the SQL queries
   and produce temporary views that are named by the keys.
3. Output - A map of keys to settings to
   create [DataFrameWriter](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html)
   objects with the `format`, `options`, `path`
   and [mode](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SaveMode.html).
   These are passed directly to the DataFrameWriter so any spark supported formats, options, paths
   and modes are available. The datasets to be written are the temporary views identified by the
   key.

For example:

```yaml
input:
  shakespeare:
    format: bigquery
    options:
      table: "bigquery-public-data:samples.shakespeare"
query:
  wordspercorpus:
    sql: "SELECT corpus, count(*) distictwords, sum(word_count) numwords FROM shakespeare GROUP by corpus ORDER BY numwords DESC"
output:
  wordspercorpus:
    format: csv
    options:
      delimiter: "\t"
    path: gs://output-bucket/words_per_corpus_tab_seperated/
```

Each input, query or output section can contain multiple entries.

Example use cases:

* multiple inputs can be transformed into new formats and output destinations in a single job
* queries can be executed to join across multiple source types.

## Run the template the general template:

Export environment variables:

```bash
export GCP_PROJECT=<project id>
export REGION=<region name>
export GCS_STAGING_LOCATION=<gcs path>
export SUBNET=projects/<project id>/regions/<region name>/subnetworks/<subnetwork name>
```

Create a config file and upload it to Cloud Storage:

```
gsutil cp config.yaml gs://bucket/path/config.yaml
```

Start the template, and provide the "submit spark" option `--files` so that it will be made locally
available to the driver:

```bash
bin/start.sh \
--files=gs://bucket/path/config.yaml \
-- \
--template GENERAL \
--config config.yaml
```

# Logging

As a workaround for a logging issue, to enable your job's application logs (outside the spark
packages)
we need to provide a `log4j-spark-driver-template.properties` file in the `--files` spark argument:

Note: this is set automatically for all templates in `start.sh`, but as this template overrides the
`--files` flag we must include it ourselves to enable logging.

eg:

```bash
bin/start.sh \
--files="gs://$GCS_STAGING_LOCATION}/log4j-spark-driver-template.properties,gs://bucket/path/config.yaml" \
-- \
--template GENERAL \
--config config.yaml
```

# Example config files:

Cloud Storage to BigQuery config.yaml

```yaml
input:
  shakespeare:
    format: bigquery
    options:
      table: "bigquery-public-data:samples.shakespeare"
output:
  shakespeare:
    format: avro
    path: gs://bucket/output/shakespeare_avro/
    mode: Overwrite
```

BigQuery to Cloud Storage config.yaml

```yaml
input:
  shakespeare:
    format: avro
    path: gs://bucket/output/shakespeare_avro/
output:
  shakespeare:
    format: bigquery
    options:
      table: "project:dataset.table"
    mode: Overwrite
```

Cloud Storage Avro to Cloud Storage CSV

```yaml
input:
  shakespeare:
    format: avro
    path: gs://bucket/output/shakespeare_avro/
output:
  shakespeare:
    format: csv
    options:
      header: true
    path: gs://bucket/output/shakespeare_csv/
    mode: Overwrite
```

Multiple inputs, queries and outputs:

```yaml
input:
  table1:
    format: avro
    path: gs://bucket/table1/
  table2:
    format: bigquery
    options:
      table: project:dataset.table2
query:
  results1:
    sql: SELECT t1.id, t1.a, t2.b FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id
  results2:
    sql: SELECT grp, count(*) cnt FROM table2 GROUP BY grp
output:
  results1:
    format: json
    path: gs://bucket/output/results1_json/
    mode: Overwrite
  results2:
    format: avro
    path: gs://bucket/output/results2_avro/
    mode: Overwrite 
```


