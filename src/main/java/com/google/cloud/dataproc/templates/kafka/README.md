## 1. Kafka To GCS

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template KAFKATOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty kafka.gcs.output.location=<gcs path> \
--templateProperty kafka.bootstrap.servers=<kafka broker list> \
--templateProperty kafka.topic=<kafka topic name> \
```


### Configurable Parameters
Update Following properties in  [template.properties](../../../../../../../resources/template.properties) file:
```
kafka.gcs.output.location=<gcs location>;
kafka.gcs.output.format=<gcs output format>;
kafka.bootstrap.servers=<kafka bootstrap server>;
kafka.topic=<kafka topic>;

#Message format can be "json" or "bytes" for bytestring
kafka.message.format=<kafka message format>

#Specifying schema is mandatory for JSON message format
kafka.schema.url=<gcs url for schema file>

#Offset to start reading from. Values can be '**earliest**' ,'latest', '<topic_name>:{<partition>:<offset>}' ...
kafka.starting.offset=<starting offset value>

#Await time in milliseconds before terminating stream read
kafka.gcs.await.termination.timeout=<stream await termination timeout>

#Ouptut mode for writing data. Values can be 'append', 'complete', 'update'
kafka.gcs.output.mode=<kafka output mode> 
```