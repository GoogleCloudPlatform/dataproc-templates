## Executing Spanner to GCS template

General Execution:

```
bin/start.sh gs://dataproc-templates/jars/shashank \
yadavaja-sandbox \
us-west1 \
projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1 \
projects/yadavaja-sandbox/regions/us-west1/clusters/per-hs \
spannertogcs
```

### Export query results as avro
Update [template.properties](../../../../../../../resources/template.properties) `table.id` property as follows:
```
table.id=(select name, age, phone from employee where designation = 'engineer')
```

**NOTE** It is required to surround your custom query with parenthesis.