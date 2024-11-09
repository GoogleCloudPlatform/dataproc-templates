## Elasticsearch to Cloud Storage

Template for exporting an Elasticsearch Index to files in Google Cloud Storage. 

It supports writing JSON, CSV, Parquet and Avro formats.

It uses the [Elasticsearch Spark Connector](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html) for reading data from Elasticsearch Index.

The template can support the Elasticsearch versions >= 7.12.0, using the appropriate Elasticsearch Spark Connector

## Arguments
- `es.gcs.input.node`: Elasticsearch Node Uri (format: mynode:9600)
- `es.gcs.input.index`: Elasticsearch Input Index Name (format: <index>/<type>)
- `es.gcs.input.user`: Elasticsearch Username
- `es.gcs.input.password`: Elasticsearch Password
- `es.gcs.input.api.key`: API Key for Elasticsearch Authorization 
- `es.gcs.output.format`: Cloud Storage Output File Format (one of: avro,parquet,csv,json)
- `es.gcs.output.location`: Cloud Storage Location to put Output Files (format: `gs://BUCKET/...`)
- `es.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

#### Optional Arguments

- `es.gcs.input.es.nodes.path.prefix`: Prefix to add to all requests made to Elasticsearch
- `es.gcs.input.es.query`: Holds the query used for reading data from the specified Index
- `es.gcs.input.es.mapping.date.rich`: Whether to create a rich Date like object for Date fields in Elasticsearch or returned them as primitives (String or long) (default true)
- `es.gcs.input.es.read.field.include`: Fields/properties that are parsed and considered when reading the documents from Elasticsearch. By default empty meaning all fields are considered
- `es.gcs.input.es.read.field.exclude`: Fields/properties that are discarded when reading the documents from Elasticsearch
- `es.gcs.input.es.read.field.as.array.include`: Fields/properties that should be considered as arrays/lists
- `es.gcs.input.es.read.field.as.array.exclude`: Fields/properties that should not be considered as arrays/lists
- `es.gcs.input.es.read.metadata`: Whether to include the document metadata (such as id and version) in the results or not in the results or not (default false)
- `es.gcs.input.es.read.metadata.field`: The field under which the metadata information is placed (default _metadata)
- `es.gcs.input.es.read.metadata.version`: Whether to include the document version in the returned metadata (default false)
- `es.gcs.input.es.index.read.missing.as.empty`: Whether elasticsearch-hadoop will allow reading of non existing indices (default no)
- `es.gcs.input.es.field.read.empty.as.null`: Whether elasticsearch-hadoop will treat empty fields as null (default yes)
- `es.gcs.input.es.read.shard.preference`: The value to use for the shard preference of a search operation when executing a scroll query
- `es.gcs.input.es.read.source.filter`: Comma delimited string of field names that you would like to return from Elasticsearch
- `es.gcs.input.es.index.read.allow.red.status`: Fetch the data from the available shards and ignore the shards which are not reachable (default false)
- `es.gcs.input.es.input.max.docs.per.partition`: The maximum number of documents per input partition. This property is a suggestion, not a guarantee
- `es.gcs.input.es.nodes.discovery`: Whether to discover the nodes within the Elasticsearch cluster or only to use the ones given in es.nodes for metadata queries (default true)
- `es.gcs.input.es.nodes.client.only`: Whether to use Elasticsearch client nodes (or load-balancers) (default false)
- `es.gcs.input.es.nodes.data.only`: Whether to use Elasticsearch data nodes only (default true)
- `es.gcs.input.es.nodes.wan.only`: Whether the connector is used against an Elasticsearch instance in a cloud/restricted environment over the WAN, such as Amazon Web Services, in order to use this option set es.gcs.input.es.nodes.discovery and es.gcs.input.es.nodes.data.only to false (default false)
- `es.gcs.input.es.http.timeout`: Timeout for HTTP/REST connections to Elasticsearch (default 1m)
- `es.gcs.input.es.http.retries`: Number of retries for establishing a (broken) http connection (default 3)
- `es.gcs.input.es.scroll.keepalive`: The maximum duration of result scrolls between query requests (default 10m)
- `es.gcs.input.es.scroll.size`: Number of results/items/documents returned per scroll request on each executor/worker/task (default 1000)
- `es.gcs.input.es.scroll.limit`: Number of total results/items returned by each individual scroll. A negative value indicates that all documents that match should be returned (default -1)
- `es.gcs.input.es.action.heart.beat.lead`: The lead to task timeout before elasticsearch-hadoop informs Hadoop the task is still running to prevent task restart (default 15s)
- `es.gcs.input.es.net.ssl`: Enable SSL (default false)
- `es.gcs.input.es.net.ssl.cert.allow.self.signed`: Whether or not to allow self signed certificates (default false)
- `es.gcs.input.es.net.ssl.protocol`: SSL protocol to be used (default TLS)
- `es.gcs.input.es.net.proxy.http.host`: Http proxy host name
- `es.gcs.input.es.net.proxy.http.port`: Http proxy port
- `es.gcs.input.es.net.proxy.http.user`: Http proxy user name
- `es.gcs.input.es.net.proxy.http.pass`: Http proxy password
- `es.gcs.input.es.net.proxy.http.use.system.props`: Whether use the system Http proxy properties (namely http.proxyHost and http.proxyPort) or not (default yes)
- `es.gcs.input.es.net.proxy.https.host`: Https proxy host name
- `es.gcs.input.es.net.proxy.https.port`: Https proxy port
- `es.gcs.input.es.net.proxy.https.user`: Https proxy user name
- `es.gcs.input.es.net.proxy.https.pass`: Https proxy password
- `es.gcs.input.es.net.proxy.https.use.system.props`: Whether use the system Https proxy properties (namely https.proxyHost and https.proxyPort) or not (default yes)
- `es.gcs.input.es.net.proxy.socks.host`: Http proxy host name
- `es.gcs.input.es.net.proxy.socks.port`: Http proxy port
- `es.gcs.input.es.net.proxy.socks.user`: Http proxy user name
- `es.gcs.input.es.net.proxy.socks.pass`: Http proxy password
- `es.gcs.input.es.net.proxy.socks.use.system.props`: Whether use the system Socks proxy properties (namely socksProxyHost and socksProxyHost) or not (default yes)
- `es.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
- `es.gcs.flatten.struct.fields`: Flatten the struct fields
- `es.gcs.flatten.array.fields`: Flatten the n-D array fields to 1-D array fields, it needs es.gcs.flatten.struct.fields option to be passed
- `es.gcs.output.compression`: None
- `es.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
- `es.gcs.output.emptyvalue`: Sets the string representation of an empty value
- `es.gcs.output.encoding`: Decodes the CSV files by the given encoding type
- `es.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
- `es.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
- `es.gcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
- `es.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
- `es.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
- `es.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
- `es.gcs.output.nullvalue`: Sets the string representation of a null value
- `es.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
- `es.gcs.output.quoteall`: None
- `es.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
- `es.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
- `es.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

**Note:** Make sure that either ```es.gcs.input.api.key``` or both ```es.gcs.input.user``` and ```es.gcs.input.password``` is provided. Setting or not setting all three properties at the same time will throw an error.

## Usage

```
$ python main.py --template ELASTICSEARCHTOGCS --help

usage: main.py [-h] 
               --es.gcs.input.node ES.GCS.INPUT.NODE
               --es.gcs.input.index ES.GCS.INPUT.INDEX
               --es.gcs.input.user ES.GCS.INPUT.USER
               --es.gcs.input.password ES.GCS.INPUT.PASSWORD
               --es.gcs.input.api.key ES.GCS.INPUT.API.KEY
               --es.gcs.output.format {avro,parquet,csv,json}
               --es.gcs.output.location ES.GCS.OUTPUT.LOCATION
               [--es.gcs.input.es.nodes.path.prefix ES.GCS.INPUT.ES.NODES.PATH.PREFIX]
               [--es.gcs.input.es.query ES.GCS.INPUT.ES.QUERY]
               [--es.gcs.input.es.mapping.date.rich ES.GCS.INPUT.ES.MAPPING.DATE.RICH]
               [--es.gcs.input.es.read.field.include ES.GCS.INPUT.ES.READ.FIELD.INCLUDE]
               [--es.gcs.input.es.read.field.exclude ES.GCS.INPUT.ES.READ.FIELD.EXCLUDE]
               [--es.gcs.input.es.read.field.as.array.include ES.GCS.INPUT.ES.READ.FIELD.AS.ARRAY.INCLUDE]
               [--es.gcs.input.es.read.field.as.array.exclude ES.GCS.INPUT.ES.READ.FIELD.AS.ARRAY.EXCLUDE]
               [--es.gcs.input.es.read.metadata ES.GCS.INPUT.ES.READ.METADATA]
               [--es.gcs.input.es.read.metadata.field ES.GCS.INPUT.ES.READ.METADATA.FIELD]
               [--es.gcs.input.es.read.metadata.version ES.GCS.INPUT.ES.READ.METADATA.VERSION]
               [--es.gcs.input.es.index.read.missing.as.empty ES.GCS.INPUT.ES.INDEX.READ.MISSING.AS.EMPTY]
               [--es.gcs.input.es.field.read.empty.as.null ES.GCS.INPUT.ES.FIELD.READ.EMPTY.AS.NULL]
               [--es.gcs.input.es.read.shard.preference ES.GCS.INPUT.ES.READ.SHARD.PREFERENCE]
               [--es.gcs.input.es.read.source.filter ES.GCS.INPUT.ES.READ.SOURCE.FILTER]
               [--es.gcs.input.es.index.read.allow.red.status ES.GCS.INPUT.ES.INDEX.READ.ALLOW.RED.STATUS]
               [--es.gcs.input.es.input.max.docs.per.partition ES.GCS.INPUT.ES.INPUT.MAX.DOCS.PER.PARTITION]
               [--es.gcs.input.es.nodes.discovery ES.GCS.INPUT.ES.NODES.DISCOVERY]
               [--es.gcs.input.es.nodes.client.only ES.GCS.INPUT.ES.NODES.CLIENT.ONLY]
               [--es.gcs.input.es.nodes.data.only ES.GCS.INPUT.ES.NODES.DATA.ONLY]
               [--es.gcs.input.es.nodes.wan.only ES.GCS.INPUT.ES.NODES.WAN.ONLY]
               [--es.gcs.input.es.http.timeout ES.GCS.INPUT.ES.HTTP.TIMEOUT]
               [--es.gcs.input.es.http.retries ES.GCS.INPUT.ES.HTTP.RETRIES]
               [--es.gcs.input.es.scroll.keepalive ES.GCS.INPUT.ES.SCROLL.KEEPALIVE]
               [--es.gcs.input.es.scroll.size ES.GCS.INPUT.ES.SCROLL.SIZE]
               [--es.gcs.input.es.scroll.limit ES.GCS.INPUT.ES.SCROLL.LIMIT]
               [--es.gcs.input.es.action.heart.beat.lead ES.GCS.INPUT.ES.ACTION.HEART.BEAT.LEAD]
               [--es.gcs.input.es.net.ssl ES.GCS.INPUT.ES.NET.SSL]
               [--es.gcs.input.es.net.ssl.cert.allow.self.signed ES.GCS.INPUT.ES.NET.SSL.CERT.ALLOW.SELF.SIGNED]
               [--es.gcs.input.es.net.ssl.protocol ES.GCS.INPUT.ES.NET.SSL.PROTOCOL]
               [--es.gcs.input.es.net.proxy.http.host ES.GCS.INPUT.ES.NET.PROXY.HTTP.HOST]
               [--es.gcs.input.es.net.proxy.http.port ES.GCS.INPUT.ES.NET.PROXY.HTTP.PORT]
               [--es.gcs.input.es.net.proxy.http.user ES.GCS.INPUT.ES.NET.PROXY.HTTP.USER]
               [--es.gcs.input.es.net.proxy.http.pass ES.GCS.INPUT.ES.NET.PROXY.HTTP.PASS]
               [--es.gcs.input.es.net.proxy.http.use.system.props ES.GCS.INPUT.ES.NET.PROXY.HTTP.USE.SYSTEM.PROPS]
               [--es.gcs.input.es.net.proxy.https.host ES.GCS.INPUT.ES.NET.PROXY.HTTPS.HOST]
               [--es.gcs.input.es.net.proxy.https.port ES.GCS.INPUT.ES.NET.PROXY.HTTPS.PORT]
               [--es.gcs.input.es.net.proxy.https.user ES.GCS.INPUT.ES.NET.PROXY.HTTPS.USER]
               [--es.gcs.input.es.net.proxy.https.pass ES.GCS.INPUT.ES.NET.PROXY.HTTPS.PASS]
               [--es.gcs.input.es.net.proxy.https.use.system.props ES.GCS.INPUT.ES.NET.PROXY.HTTPS.USE.SYSTEM.PROPS]
               [--es.gcs.input.es.net.proxy.socks.host ES.GCS.INPUT.ES.NET.PROXY.SOCKS.HOST]
               [--es.gcs.input.es.net.proxy.socks.port ES.GCS.INPUT.ES.NET.PROXY.SOCKS.PORT]
               [--es.gcs.input.es.net.proxy.socks.user ES.GCS.INPUT.ES.NET.PROXY.SOCKS.USER]
               [--es.gcs.input.es.net.proxy.socks.pass ES.GCS.INPUT.ES.NET.PROXY.SOCKS.PASS]
               [--es.gcs.input.es.net.proxy.socks.use.system.props ES.GCS.INPUT.ES.NET.PROXY.SOCKS.USE.SYSTEM.PROPS]
               [--es.gcs.flatten.struct.fields]
               [--es.gcs.flatten.array.fields]
               [--es.gcs.output.mode {overwrite,append,ignore,errorifexists}]
               [--es.gcs.output.chartoescapequoteescaping ES.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--es.gcs.output.compression ES.GCS.OUTPUT.COMPRESSION]
               [--es.gcs.output.dateformat ES.GCS.OUTPUT.DATEFORMAT]
               [--es.gcs.output.emptyvalue ES.GCS.OUTPUT.EMPTYVALUE]
               [--es.gcs.output.encoding ES.GCS.OUTPUT.ENCODING]
               [--es.gcs.output.escape ES.GCS.OUTPUT.ESCAPE]
               [--es.gcs.output.escapequotes ES.GCS.OUTPUT.ESCAPEQUOTES]
               [--es.gcs.output.header ES.GCS.OUTPUT.HEADER]
               [--es.gcs.output.ignoreleadingwhitespace ES.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--es.gcs.output.ignoretrailingwhitespace ES.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--es.gcs.output.linesep ES.GCS.OUTPUT.LINESEP]
               [--es.gcs.output.nullvalue ES.GCS.OUTPUT.NULLVALUE]
               [--es.gcs.output.quote ES.GCS.OUTPUT.QUOTE] [--es.gcs.output.quoteall ES.GCS.OUTPUT.QUOTEALL]
               [--es.gcs.output.sep ES.GCS.OUTPUT.SEP]
               [--es.gcs.output.timestampformat ES.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--es.gcs.output.timestampntzformat ES.GCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --es.gcs.input.node ES.GCS.INPUT.NODE
                        Elasticsearch Node Uri
  --es.gcs.input.index ES.GCS.INPUT.INDEX
                        Elasticsearch Input Index Name
  --es.gcs.input.user ES.GCS.INPUT.USER
                        Elasticsearch Username
  --es.gcs.input.password ES.GCS.INPUT.PASSWORD
                        Elasticsearch Password   
  --es.gcs.input.api.key ES.GCS.INPUT.API.KEY
                        API Key for Elasticsearch Authorization  
  --es.gcs.input.es.nodes.path.prefix ES.GCS.INPUT.ES.NODES.PATH.PREFIX
                        Prefix to add to all requests made to Elasticsearch
  --es.gcs.input.es.query ES.GCS.INPUT.ES.QUERY
                        Holds the query used for reading data from the specified Index
  --es.gcs.input.es.mapping.date.rich ES.GCS.INPUT.ES.MAPPING.DATE.RICH
                        Whether to create a rich Date like object for Date fields in Elasticsearch or returned them as primitives (String or long)
  --es.gcs.input.es.read.field.include ES.GCS.INPUT.ES.READ.FIELD.INCLUDE
                        Fields/properties that are parsed and considered when reading the documents from Elasticsearch. By default empty meaning all fields are considered
  --es.gcs.input.es.read.field.exclude ES.GCS.INPUT.ES.READ.FIELD.EXCLUDE
                        Fields/properties that are discarded when reading the documents from Elasticsearch. By default empty meaning no fields are excluded
  --es.gcs.input.es.read.field.as.array.include ES.GCS.INPUT.ES.READ.FIELD.AS.ARRAY.INCLUDE
                        Fields/properties that should be considered as arrays/lists
  --es.gcs.input.es.read.field.as.array.exclude ES.GCS.INPUT.ES.READ.FIELD.AS.ARRAY.EXCLUDE
                        Fields/properties that should not be considered as arrays/lists
  --es.gcs.input.es.read.metadata ES.GCS.INPUT.ES.READ.METADATA
                        Whether to include the document metadata (such as id and version) in the results or not in the results or not
  --es.gcs.input.es.read.metadata.field ES.GCS.INPUT.ES.READ.METADATA.FIELD
                        The field under which the metadata information is placed
  --es.gcs.input.es.read.metadata.version ES.GCS.INPUT.ES.READ.METADATA.VERSION
                        Whether to include the document version in the returned metadata
  --es.gcs.input.es.index.read.missing.as.empty ES.GCS.INPUT.ES.INDEX.READ.MISSING.AS.EMPTY
                        Whether elasticsearch-hadoop will allow reading of non existing indices
  --es.gcs.input.es.field.read.empty.as.null ES.GCS.INPUT.ES.FIELD.READ.EMPTY.AS.NULL
                        Whether elasticsearch-hadoop will treat empty fields as null
  --es.gcs.input.es.read.shard.preference ES.GCS.INPUT.ES.READ.SHARD.PREFERENCE
                        The value to use for the shard preference of a search operation when executing a scroll query
  --es.gcs.input.es.read.source.filter ES.GCS.INPUT.ES.READ.SOURCE.FILTER
                        Comma delimited string of field names that you would like to return from Elasticsearch
  --es.gcs.input.es.index.read.allow.red.status ES.GCS.INPUT.ES.INDEX.READ.ALLOW.RED.STATUS
                        Fetch the data from the available shards and ignore the shards which are not reachable
  --es.gcs.input.es.input.max.docs.per.partition ES.GCS.INPUT.ES.INPUT.MAX.DOCS.PER.PARTITION
                        The maximum number of documents per input partition. This property is a suggestion, not a guarantee
  --es.gcs.input.es.nodes.discovery ES.GCS.INPUT.ES.NODES.DISCOVERY
                        Whether to discover the nodes within the Elasticsearch cluster or only to use the ones given in es.nodes for metadata queries
  --es.gcs.input.es.nodes.client.only ES.GCS.INPUT.ES.NODES.CLIENT.ONLY
                        Whether to use Elasticsearch client nodes (or load-balancers)
  --es.gcs.input.es.nodes.data.only ES.GCS.INPUT.ES.NODES.DATA.ONLY
                        Whether to use Elasticsearch data nodes only
  --es.gcs.input.es.nodes.wan.only ES.GCS.INPUT.ES.NODES.WAN.ONLY
                        Whether the connector is used against an Elasticsearch instance in a cloud/restricted environment over the WAN, such as Amazon Web Services, in order to use this option set es.gcs.input.es.nodes.discovery and es.gcs.input.es.nodes.data.only to false
  --es.gcs.input.es.http.timeout ES.GCS.INPUT.ES.HTTP.TIMEOUT
                        Timeout for HTTP/REST connections to Elasticsearch
  --es.gcs.input.es.http.retries ES.GCS.INPUT.ES.HTTP.RETRIES
                        Number of retries for establishing a (broken) http connection
  --es.gcs.input.es.scroll.keepalive ES.GCS.INPUT.ES.SCROLL.KEEPALIVE
                        The maximum duration of result scrolls between query requests
  --es.gcs.input.es.scroll.size ES.GCS.INPUT.ES.SCROLL.SIZE
                        Number of results/items/documents returned per scroll request on each executor/worker/task
  --es.gcs.input.es.scroll.limit ES.GCS.INPUT.ES.SCROLL.LIMIT
                        Number of total results/items returned by each individual scroll. A negative value indicates that all documents that match should be returned
  --es.gcs.input.es.action.heart.beat.lead ES.GCS.INPUT.ES.ACTION.HEART.BEAT.LEAD
                        The lead to task timeout before elasticsearch-hadoop informs Hadoop the task is still running to prevent task restart
  --es.gcs.input.es.net.ssl ES.GCS.INPUT.ES.NET.SSL
                        Enable SSL
  --es.gcs.input.es.net.ssl.cert.allow.self.signed ES.GCS.INPUT.ES.NET.SSL.CERT.ALLOW.SELF.SIGNED
                        Whether or not to allow self signed certificates
  --es.gcs.input.es.net.ssl.protocol ES.GCS.INPUT.ES.NET.SSL.PROTOCOL
                        SSL protocol to be used
  --es.gcs.input.es.net.proxy.http.host ES.GCS.INPUT.ES.NET.PROXY.HTTP.HOST
                        Http proxy host name
  --es.gcs.input.es.net.proxy.http.port ES.GCS.INPUT.ES.NET.PROXY.HTTP.PORT
                        Http proxy port
  --es.gcs.input.es.net.proxy.http.user ES.GCS.INPUT.ES.NET.PROXY.HTTP.USER
                        Http proxy user name
  --es.gcs.input.es.net.proxy.http.pass ES.GCS.INPUT.ES.NET.PROXY.HTTP.PASS
                        Http proxy password
  --es.gcs.input.es.net.proxy.http.use.system.props ES.GCS.INPUT.ES.NET.PROXY.HTTP.USE.SYSTEM.PROPS
                        Whether use the system Http proxy properties (namely http.proxyHost and http.proxyPort) or not
  --es.gcs.input.es.net.proxy.https.host ES.GCS.INPUT.ES.NET.PROXY.HTTPS.HOST
                        Https proxy host name
  --es.gcs.input.es.net.proxy.https.port ES.GCS.INPUT.ES.NET.PROXY.HTTPS.PORT
                        Https proxy port
  --es.gcs.input.es.net.proxy.https.user ES.GCS.INPUT.ES.NET.PROXY.HTTPS.USER
                        Https proxy user name
  --es.gcs.input.es.net.proxy.https.pass ES.GCS.INPUT.ES.NET.PROXY.HTTPS.PASS
                        Https proxy password
  --es.gcs.input.es.net.proxy.https.use.system.props ES.GCS.INPUT.ES.NET.PROXY.HTTPS.USE.SYSTEM.PROPS
                        Whether use the system Https proxy properties (namely https.proxyHost and https.proxyPort) or not
  --es.gcs.input.es.net.proxy.socks.host ES.GCS.INPUT.ES.NET.PROXY.SOCKS.HOST
                        Http proxy host name
  --es.gcs.input.es.net.proxy.socks.port ES.GCS.INPUT.ES.NET.PROXY.SOCKS.PORT
                        Http proxy port
   --es.gcs.input.es.net.proxy.socks.user ES.GCS.INPUT.ES.NET.PROXY.SOCKS.USER
                        Http proxy user name
  --es.gcs.input.es.net.proxy.socks.pass ES.GCS.INPUT.ES.NET.PROXY.SOCKS.PASS
                        Http proxy password
  --es.gcs.input.es.net.proxy.socks.use.system.props ES.GCS.INPUT.ES.NET.PROXY.SOCKS.USE.SYSTEM.PROPS
                        Whether use the system Socks proxy properties (namely socksProxyHost and socksProxyHost) or not     
  --es.gcs.flatten.struct.fields
                        Flatten the struct fields
  --es.gcs.flatten.array.fields
                        Flatten the n-D array fields to 1-D array fields, it needs es.gcs.flatten.struct.fields option to be passed      
  --es.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --es.gcs.output.location ES.GCS.OUTPUT.LOCATION
                        Cloud Storage location for output files
  --es.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --es.gcs.output.chartoescapequoteescaping ES.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --es.gcs.output.compression ES.GCS.OUTPUT.COMPRESSION
  --es.gcs.output.dateformat ES.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --es.gcs.output.emptyvalue ES.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --es.gcs.output.encoding ES.GCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --es.gcs.output.escape ES.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --es.gcs.output.escapequotes ES.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --es.gcs.output.header ES.GCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --es.gcs.output.ignoreleadingwhitespace ES.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --es.gcs.output.ignoretrailingwhitespace ES.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --es.gcs.output.linesep ES.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --es.gcs.output.nullvalue ES.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --es.gcs.output.quote ES.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --es.gcs.output.quoteall ES.GCS.OUTPUT.QUOTEALL
  --es.gcs.output.sep ES.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --es.gcs.output.timestampformat ES.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --es.gcs.output.timestampntzformat ES.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
```

## Required JAR files

This template requires the [Elasticsearch Spark Connector](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html) to be available in the Dataproc cluster. 

Depending upon the versions of Elasticsearch, PySpark and Scala in the environment the Elasticsearch Spark JAR can be downloaded from this [link](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-30). 

The template can support the Elasticsearch versions >= 7.12.0

## Example submission

```
export GCP_PROJECT=my-project
export JARS="gs://<your_bucket_to_store_dependencies>/elasticsearch-spark-30_2.12-8.11.4.jar"
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
-- --template=ELASTICSEARCHTOGCS \
    --es.gcs.input.node="xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243" \
    --es.gcs.input.index="demo" \
    --es.gcs.input.user="demo" \
    --es.gcs.input.password="demo" \
    --es.gcs.output.format="parquet" \
    --es.gcs.output.location="gs://my-output/esgcsoutput" \
    --es.gcs.output.mode="overwrite"
```

# Elasticsearch To BigQuery

Template for exporting an Elasticsearch Index to a BigQuery table.

## Required JAR files

It uses the [Elasticsearch Spark Connector](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html) for reading data from Elasticsearch Index. To write to BigQuery, the template needs [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector).

Depending upon the versions of Elasticsearch, PySpark and Scala in the environment the Elasticsearch Spark JAR can be downloaded from this [link](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-30). 

The template can support the Elasticsearch versions >= 7.12.0, using the appropriate Elasticsearch Spark Connector

This template has been tested with the following versions of the above mentioned JAR files:

1. Elasticsearch Spark Connector: 8.11.4
2. Spark BigQuery Connector: 0.39

## Arguments

- `es.bq.input.node`: Elasticsearch Node Uri (format: mynode:9600)
- `es.bq.input.index`: Elasticsearch Input Index Name (format: <index>/<type>)
- `es.bq.input.user`: Elasticsearch Username
- `es.bq.input.password`: Elasticsearch Password
- `es.bq.input.api.key`: API Key for Elasticsearch Authorization 
- `es.bq.output.dataset`: BigQuery dataset id (format: Dataset_id)
- `es.bq.output.table`: BigQuery table name (format: Table_name)
- `es.bq.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector

#### Optional Arguments

- `es.bq.input.es.nodes.path.prefix`: Prefix to add to all requests made to Elasticsearch
- `es.bq.input.es.query`: Holds the query used for reading data from the specified Index
- `es.bq.input.es.mapping.date.rich`: Whether to create a rich Date like object for Date fields in Elasticsearch or returned them as primitives (String or long) (default true)
- `es.bq.input.es.read.field.include`: Fields/properties that are parsed and considered when reading the documents from Elasticsearch. By default empty meaning all fields are considered
- `es.bq.input.es.read.field.exclude`: Fields/properties that are discarded when reading the documents from Elasticsearch
- `es.bq.input.es.read.field.as.array.include`: Fields/properties that should be considered as arrays/lists
- `es.bq.input.es.read.field.as.array.exclude`: Fields/properties that should not be considered as arrays/lists
- `es.bq.input.es.read.metadata`: Whether to include the document metadata (such as id and version) in the results or not in the results or not (default false)
- `es.bq.input.es.read.metadata.field`: The field under which the metadata information is placed (default _metadata)
- `es.bq.input.es.read.metadata.version`: Whether to include the document version in the returned metadata (default false)
- `es.bq.input.es.index.read.missing.as.empty`: Whether elasticsearch-hadoop will allow reading of non existing indices (default no)
- `es.bq.input.es.field.read.empty.as.null`: Whether elasticsearch-hadoop will treat empty fields as null (default yes)
- `es.bq.input.es.read.shard.preference`: The value to use for the shard preference of a search operation when executing a scroll query
- `es.bq.input.es.read.source.filter`: Comma delimited string of field names that you would like to return from Elasticsearch
- `es.bq.input.es.index.read.allow.red.status`: Fetch the data from the available shards and ignore the shards which are not reachable (default false)
- `es.bq.input.es.input.max.docs.per.partition`: The maximum number of documents per input partition. This property is a suggestion, not a guarantee
- `es.bq.input.es.nodes.discovery`: Whether to discover the nodes within the Elasticsearch cluster or only to use the ones given in es.nodes for metadata queries (default true)
- `es.bq.input.es.nodes.client.only`: Whether to use Elasticsearch client nodes (or load-balancers) (default false)
- `es.bq.input.es.nodes.data.only`: Whether to use Elasticsearch data nodes only (default true)
- `es.bq.input.es.nodes.wan.only`: Whether the connector is used against an Elasticsearch instance in a cloud/restricted environment over the WAN, such as Amazon Web Services, in order to use this option set es.bq.input.es.nodes.discovery and es.bq.input.es.nodes.data.only to false (default false)
- `es.bq.input.es.http.timeout`: Timeout for HTTP/REST connections to Elasticsearch (default 1m)
- `es.bq.input.es.http.retries`: Number of retries for establishing a (broken) http connection (default 3)
- `es.bq.input.es.scroll.keepalive`: The maximum duration of result scrolls between query requests (default 10m)
- `es.bq.input.es.scroll.size`: Number of results/items/documents returned per scroll request on each executor/worker/task (default 1000)
- `es.bq.input.es.scroll.limit`: Number of total results/items returned by each individual scroll. A negative value indicates that all documents that match should be returned (default -1)
- `es.bq.input.es.action.heart.beat.lead`: The lead to task timeout before elasticsearch-hadoop informs Hadoop the task is still running to prevent task restart (default 15s)
- `es.bq.input.es.net.ssl`: Enable SSL (default false)
- `es.bq.input.es.net.ssl.cert.allow.self.signed`: Whether or not to allow self signed certificates (default false)
- `es.bq.input.es.net.ssl.protocol`: SSL protocol to be used (default TLS)
- `es.bq.input.es.net.proxy.http.host`: Http proxy host name
- `es.bq.input.es.net.proxy.http.port`: Http proxy port
- `es.bq.input.es.net.proxy.http.user`: Http proxy user name
- `es.bq.input.es.net.proxy.http.pass`: Http proxy password
- `es.bq.input.es.net.proxy.http.use.system.props`: Whether use the system Http proxy properties (namely http.proxyHost and http.proxyPort) or not (default yes)
- `es.bq.input.es.net.proxy.https.host`: Https proxy host name
- `es.bq.input.es.net.proxy.https.port`: Https proxy port
- `es.bq.input.es.net.proxy.https.user`: Https proxy user name
- `es.bq.input.es.net.proxy.https.pass`: Https proxy password
- `es.bq.input.es.net.proxy.https.use.system.props`: Whether use the system Https proxy properties (namely https.proxyHost and https.proxyPort) or not (default yes)
- `es.bq.input.es.net.proxy.socks.host`: Http proxy host name
- `es.bq.input.es.net.proxy.socks.port`: Http proxy port
- `es.bq.input.es.net.proxy.socks.user`: Http proxy user name
- `es.bq.input.es.net.proxy.socks.pass`: Http proxy password
- `es.bq.input.es.net.proxy.socks.use.system.props`: Whether use the system Socks proxy properties (namely socksProxyHost and socksProxyHost) or not (default yes)
- `es.bq.flatten.struct.fields`: Flatten the struct fields
- `es.bq.flatten.array.fields`: Flatten the n-D array fields to 1-D array fields, it needs es.bq.flatten.struct.fields option to be passed
- `es.bq.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

**Note:** Make sure that either ```es.bq.input.api.key``` or both ```es.bq.input.user``` and ```es.bq.input.password``` is provided. Setting or not setting all three properties at the same time will throw an error.

## Usage

```
$ python main.py --template ELASTICSEARCHTOBQ --help
usage: main.py [-h] 
               --es.bq.input.node ES.BQ.INPUT.NODE
               --es.bq.input.index ES.BQ.INPUT.INDEX
               --es.bq.input.user ES.BQ.INPUT.USER
               --es.bq.input.password ES.BQ.INPUT.PASSWORD
               --es.bq.input.api.key ES.BQ.INPUT.API.KEY
               --es.bq.output.dataset ES.BQ.OUTPUT.DATASET
               --es.bq.output.table ES.BQ.OUTPUT.TABLE
               --es.bq.temp.bucket.name ES.BQ.TEMP.BUCKET.NAME
               --es.bq.output.mode {overwrite,append,ignore,errorifexists}
               [--es.bq.input.es.nodes.path.prefix ES.BQ.INPUT.ES.NODES.PATH.PREFIX]
               [--es.bq.input.es.query ES.BQ.INPUT.ES.QUERY]
               [--es.bq.input.es.mapping.date.rich ES.BQ.INPUT.ES.MAPPING.DATE.RICH]
               [--es.bq.input.es.read.field.include ES.BQ.INPUT.ES.READ.FIELD.INCLUDE]
               [--es.bq.input.es.read.field.exclude ES.BQ.INPUT.ES.READ.FIELD.EXCLUDE]
               [--es.bq.input.es.read.field.as.array.include ES.BQ.INPUT.ES.READ.FIELD.AS.ARRAY.INCLUDE]
               [--es.bq.input.es.read.field.as.array.exclude ES.BQ.INPUT.ES.READ.FIELD.AS.ARRAY.EXCLUDE]
               [--es.bq.input.es.read.metadata ES.BQ.INPUT.ES.READ.METADATA]
               [--es.bq.input.es.read.metadata.field ES.BQ.INPUT.ES.READ.METADATA.FIELD]
               [--es.bq.input.es.read.metadata.version ES.BQ.INPUT.ES.READ.METADATA.VERSION]
               [--es.bq.input.es.index.read.missing.as.empty ES.BQ.INPUT.ES.INDEX.READ.MISSING.AS.EMPTY]
               [--es.bq.input.es.field.read.empty.as.null ES.BQ.INPUT.ES.FIELD.READ.EMPTY.AS.NULL]
               [--es.bq.input.es.read.shard.preference ES.BQ.INPUT.ES.READ.SHARD.PREFERENCE]
               [--es.bq.input.es.read.source.filter ES.BQ.INPUT.ES.READ.SOURCE.FILTER]
               [--es.bq.input.es.index.read.allow.red.status ES.BQ.INPUT.ES.INDEX.READ.ALLOW.RED.STATUS]
               [--es.bq.input.es.input.max.docs.per.partition ES.BQ.INPUT.ES.INPUT.MAX.DOCS.PER.PARTITION]
               [--es.bq.input.es.nodes.discovery ES.BQ.INPUT.ES.NODES.DISCOVERY]
               [--es.bq.input.es.nodes.client.only ES.BQ.INPUT.ES.NODES.CLIENT.ONLY]
               [--es.bq.input.es.nodes.data.only ES.BQ.INPUT.ES.NODES.DATA.ONLY]
               [--es.bq.input.es.nodes.wan.only ES.BQ.INPUT.ES.NODES.WAN.ONLY]
               [--es.bq.input.es.http.timeout ES.BQ.INPUT.ES.HTTP.TIMEOUT]
               [--es.bq.input.es.http.retries ES.BQ.INPUT.ES.HTTP.RETRIES]
               [--es.bq.input.es.scroll.keepalive ES.BQ.INPUT.ES.SCROLL.KEEPALIVE]
               [--es.bq.input.es.scroll.size ES.BQ.INPUT.ES.SCROLL.SIZE]
               [--es.bq.input.es.scroll.limit ES.BQ.INPUT.ES.SCROLL.LIMIT]
               [--es.bq.input.es.action.heart.beat.lead ES.BQ.INPUT.ES.ACTION.HEART.BEAT.LEAD]
               [--es.bq.input.es.net.ssl ES.BQ.INPUT.ES.NET.SSL]
               [--es.bq.input.es.net.ssl.cert.allow.self.signed ES.BQ.INPUT.ES.NET.SSL.CERT.ALLOW.SELF.SIGNED]
               [--es.bq.input.es.net.ssl.protocol ES.BQ.INPUT.ES.NET.SSL.PROTOCOL]
               [--es.bq.input.es.net.proxy.http.host ES.BQ.INPUT.ES.NET.PROXY.HTTP.HOST]
               [--es.bq.input.es.net.proxy.http.port ES.BQ.INPUT.ES.NET.PROXY.HTTP.PORT]
               [--es.bq.input.es.net.proxy.http.user ES.BQ.INPUT.ES.NET.PROXY.HTTP.USER]
               [--es.bq.input.es.net.proxy.http.pass ES.BQ.INPUT.ES.NET.PROXY.HTTP.PASS]
               [--es.bq.input.es.net.proxy.http.use.system.props ES.BQ.INPUT.ES.NET.PROXY.HTTP.USE.SYSTEM.PROPS]
               [--es.bq.input.es.net.proxy.https.host ES.BQ.INPUT.ES.NET.PROXY.HTTPS.HOST]
               [--es.bq.input.es.net.proxy.https.port ES.BQ.INPUT.ES.NET.PROXY.HTTPS.PORT]
               [--es.bq.input.es.net.proxy.https.user ES.BQ.INPUT.ES.NET.PROXY.HTTPS.USER]
               [--es.bq.input.es.net.proxy.https.pass ES.BQ.INPUT.ES.NET.PROXY.HTTPS.PASS]
               [--es.bq.input.es.net.proxy.https.use.system.props ES.BQ.INPUT.ES.NET.PROXY.HTTPS.USE.SYSTEM.PROPS]
               [--es.bq.input.es.net.proxy.socks.host ES.BQ.INPUT.ES.NET.PROXY.SOCKS.HOST]
               [--es.bq.input.es.net.proxy.socks.port ES.BQ.INPUT.ES.NET.PROXY.SOCKS.PORT]
               [--es.bq.input.es.net.proxy.socks.user ES.BQ.INPUT.ES.NET.PROXY.SOCKS.USER]
               [--es.bq.input.es.net.proxy.socks.pass ES.BQ.INPUT.ES.NET.PROXY.SOCKS.PASS]
               [--es.bq.input.es.net.proxy.socks.use.system.props ES.BQ.INPUT.ES.NET.PROXY.SOCKS.USE.SYSTEM.PROPS]
               [--es.bq.flatten.struct.fields]
               [--es.bq.flatten.array.fields]

options:
  -h, --help            show this help message and exit
  --es.bq.input.node ES.BQ.INPUT.NODE
                        Elasticsearch Node Uri
  --es.bq.input.index ES.BQ.INPUT.INDEX
                        Elasticsearch Input Index Name
  --es.bq.input.user ES.BQ.INPUT.USER
                        Elasticsearch Username
  --es.bq.input.password ES.BQ.INPUT.PASSWORD
                        Elasticsearch Password   
  --es.bq.input.api.key ES.BQ.INPUT.API.KEY
                        API Key for Elasticsearch Authorization 
  --es.bq.input.es.nodes.path.prefix ES.BQ.INPUT.ES.NODES.PATH.PREFIX
                        Prefix to add to all requests made to Elasticsearch
  --es.bq.input.es.query ES.BQ.INPUT.ES.QUERY
                        Holds the query used for reading data from the specified Index
  --es.bq.input.es.mapping.date.rich ES.BQ.INPUT.ES.MAPPING.DATE.RICH
                        Whether to create a rich Date like object for Date fields in Elasticsearch or returned them as primitives (String or long)
  --es.bq.input.es.read.field.include ES.BQ.INPUT.ES.READ.FIELD.INCLUDE
                        Fields/properties that are parsed and considered when reading the documents from Elasticsearch. By default empty meaning all fields are considered
  --es.bq.input.es.read.field.exclude ES.BQ.INPUT.ES.READ.FIELD.EXCLUDE
                        Fields/properties that are discarded when reading the documents from Elasticsearch. By default empty meaning no fields are excluded
  --es.bq.input.es.read.field.as.array.include ES.BQ.INPUT.ES.READ.FIELD.AS.ARRAY.INCLUDE
                        Fields/properties that should be considered as arrays/lists
  --es.bq.input.es.read.field.as.array.exclude ES.BQ.INPUT.ES.READ.FIELD.AS.ARRAY.EXCLUDE
                        Fields/properties that should not be considered as arrays/lists
  --es.bq.input.es.read.metadata ES.BQ.INPUT.ES.READ.METADATA
                        Whether to include the document metadata (such as id and version) in the results or not in the results or not
  --es.bq.input.es.read.metadata.field ES.BQ.INPUT.ES.READ.METADATA.FIELD
                        The field under which the metadata information is placed
  --es.bq.input.es.read.metadata.version ES.BQ.INPUT.ES.READ.METADATA.VERSION
                        Whether to include the document version in the returned metadata
  --es.bq.input.es.index.read.missing.as.empty ES.BQ.INPUT.ES.INDEX.READ.MISSING.AS.EMPTY
                        Whether elasticsearch-hadoop will allow reading of non existing indices
  --es.bq.input.es.field.read.empty.as.null ES.BQ.INPUT.ES.FIELD.READ.EMPTY.AS.NULL
                        Whether elasticsearch-hadoop will treat empty fields as null
  --es.bq.input.es.read.shard.preference ES.BQ.INPUT.ES.READ.SHARD.PREFERENCE
                        The value to use for the shard preference of a search operation when executing a scroll query
  --es.bq.input.es.read.source.filter ES.BQ.INPUT.ES.READ.SOURCE.FILTER
                        Comma delimited string of field names that you would like to return from Elasticsearch
  --es.bq.input.es.index.read.allow.red.status ES.BQ.INPUT.ES.INDEX.READ.ALLOW.RED.STATUS
                        Fetch the data from the available shards and ignore the shards which are not reachable
  --es.bq.input.es.input.max.docs.per.partition ES.BQ.INPUT.ES.INPUT.MAX.DOCS.PER.PARTITION
                        The maximum number of documents per input partition. This property is a suggestion, not a guarantee
  --es.bq.input.es.nodes.discovery ES.BQ.INPUT.ES.NODES.DISCOVERY
                        Whether to discover the nodes within the Elasticsearch cluster or only to use the ones given in es.nodes for metadata queries
  --es.bq.input.es.nodes.client.only ES.BQ.INPUT.ES.NODES.CLIENT.ONLY
                        Whether to use Elasticsearch client nodes (or load-balancers)
  --es.bq.input.es.nodes.data.only ES.BQ.INPUT.ES.NODES.DATA.ONLY
                        Whether to use Elasticsearch data nodes only
  --es.bq.input.es.nodes.wan.only ES.BQ.INPUT.ES.NODES.WAN.ONLY
                        Whether the connector is used against an Elasticsearch instance in a cloud/restricted environment over the WAN, such as Amazon Web Services, in order to use this option set es.bq.input.es.nodes.discovery and es.bq.input.es.nodes.data.only to false
  --es.bq.input.es.http.timeout ES.BQ.INPUT.ES.HTTP.TIMEOUT
                        Timeout for HTTP/REST connections to Elasticsearch
  --es.bq.input.es.http.retries ES.BQ.INPUT.ES.HTTP.RETRIES
                        Number of retries for establishing a (broken) http connection
  --es.bq.input.es.scroll.keepalive ES.BQ.INPUT.ES.SCROLL.KEEPALIVE
                        The maximum duration of result scrolls between query requests
  --es.bq.input.es.scroll.size ES.BQ.INPUT.ES.SCROLL.SIZE
                        Number of results/items/documents returned per scroll request on each executor/worker/task
  --es.bq.input.es.scroll.limit ES.BQ.INPUT.ES.SCROLL.LIMIT
                        Number of total results/items returned by each individual scroll. A negative value indicates that all documents that match should be returned
  --es.bq.input.es.action.heart.beat.lead ES.BQ.INPUT.ES.ACTION.HEART.BEAT.LEAD
                        The lead to task timeout before elasticsearch-hadoop informs Hadoop the task is still running to prevent task restart
  --es.bq.input.es.net.ssl ES.BQ.INPUT.ES.NET.SSL
                        Enable SSL
  --es.bq.input.es.net.ssl.cert.allow.self.signed ES.BQ.INPUT.ES.NET.SSL.CERT.ALLOW.SELF.SIGNED
                        Whether or not to allow self signed certificates
  --es.bq.input.es.net.ssl.protocol ES.BQ.INPUT.ES.NET.SSL.PROTOCOL
                        SSL protocol to be used
  --es.bq.input.es.net.proxy.http.host ES.BQ.INPUT.ES.NET.PROXY.HTTP.HOST
                        Http proxy host name
  --es.bq.input.es.net.proxy.http.port ES.BQ.INPUT.ES.NET.PROXY.HTTP.PORT
                        Http proxy port
  --es.bq.input.es.net.proxy.http.user ES.BQ.INPUT.ES.NET.PROXY.HTTP.USER
                        Http proxy user name
  --es.bq.input.es.net.proxy.http.pass ES.BQ.INPUT.ES.NET.PROXY.HTTP.PASS
                        Http proxy password
  --es.bq.input.es.net.proxy.http.use.system.props ES.BQ.INPUT.ES.NET.PROXY.HTTP.USE.SYSTEM.PROPS
                        Whether use the system Http proxy properties (namely http.proxyHost and http.proxyPort) or not
  --es.bq.input.es.net.proxy.https.host ES.BQ.INPUT.ES.NET.PROXY.HTTPS.HOST
                        Https proxy host name
  --es.bq.input.es.net.proxy.https.port ES.BQ.INPUT.ES.NET.PROXY.HTTPS.PORT
                        Https proxy port
  --es.bq.input.es.net.proxy.https.user ES.BQ.INPUT.ES.NET.PROXY.HTTPS.USER
                        Https proxy user name
  --es.bq.input.es.net.proxy.https.pass ES.BQ.INPUT.ES.NET.PROXY.HTTPS.PASS
                        Https proxy password
  --es.bq.input.es.net.proxy.https.use.system.props ES.BQ.INPUT.ES.NET.PROXY.HTTPS.USE.SYSTEM.PROPS
                        Whether use the system Https proxy properties (namely https.proxyHost and https.proxyPort) or not
  --es.bq.input.es.net.proxy.socks.host ES.BQ.INPUT.ES.NET.PROXY.SOCKS.HOST
                        Http proxy host name
  --es.bq.input.es.net.proxy.socks.port ES.BQ.INPUT.ES.NET.PROXY.SOCKS.PORT
                        Http proxy port
   --es.bq.input.es.net.proxy.socks.user ES.BQ.INPUT.ES.NET.PROXY.SOCKS.USER
                        Http proxy user name
  --es.bq.input.es.net.proxy.socks.pass ES.BQ.INPUT.ES.NET.PROXY.SOCKS.PASS
                        Http proxy password
  --es.bq.input.es.net.proxy.socks.use.system.props ES.BQ.INPUT.ES.NET.PROXY.SOCKS.USE.SYSTEM.PROPS
                        Whether use the system Socks proxy properties (namely socksProxyHost and socksProxyHost) or not     
  --es.bq.flatten.struct.fields
                        Flatten the struct fields
  --es.bq.flatten.array.fields
                        Flatten the n-D array fields to 1-D array fields, it needs es.bq.flatten.struct.fields option to be passed
  --es.bq.output.dataset ES.BQ.OUTPUT.DATASET
                        BigQuery Output Dataset Name
  --es.bq.output.table ES.BQ.OUTPUT.TABLE
                        BigQuery Output Table Name
  --es.bq.temp.bucket.name ES.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --es.bq.output.mode {overwrite,append,ignore,errorifexists}
                        BigQuery Output write mode (one of:
                        append,overwrite,ignore,errorifexists) (Defaults to
                        append)

```

## Example submission

```
export GCP_PROJECT=my-project
export GCS_STAGING_LOCATION="gs://my-bucket"
export JARS="gs://<your_bucket_to_store_dependencies>/elasticsearch-spark-30_2.12-8.11.4.jar,gs://<your_bucket_to_store_dependencies>/spark-3.3-bigquery-0.39.0.jar"
export REGION=us-central1
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
-- --template=ELASTICSEARCHTOBQ \
    --es.bq.input.node="xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243" \
    --es.bq.input.index="demo" \
    --es.bq.input.user="demo" \
    --es.bq.input.password="demo" \
    --es.bq.output.dataset="my-project.test_dataset" \
    --es.bq.output.table="dummyusers" \
    --es.bq.temp.bucket.name="<temp-bq-bucket-name>" \
    --es.bq.output.mode="append"
```
# Elasticsearch To Bigtable

Template for exporting an Elasticsearch Index to a BigTable table.

It uses the Spark BigTable Connector to write to Bigtable.

Here in this template, you will notice that there are different configuration steps for the PySpark job to successfully run using Dataproc Serverless, connecting to Bigtable using Bigtable connector.

You can also check out the [differences between HBase and Cloud Bigtable](https://cloud.google.com/bigtable/docs/hbase-differences).

## Requirements

1) `export JARS="gs://spark-lib/bigtable/spark-bigtable_2.12-0.1.0.jar"` and also required `spark.jars.packages=org.slf4j:slf4j-reload4j:1.7.36` Please refer example from official [documentation](https://github.com/GoogleCloudDataproc/spark-bigtable-connector/tree/main/examples/python).

2) Configure the desired BigTable catalog json to passed as an argument (table reference and schema)
    - The catalog.json should be passed using the --es.bt.catalog.json
    ```
    (./bin/start.sh ...)
    -- --es.bt.catalog.json='''{
                        "table":{"name":"<table_id>"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
    ```

3) [Create and manage](https://cloud.google.com/bigtable/docs/managing-tables) your Bigtable table schema, column families, etc, to match the provided Bigtable catalog.

## Required JAR files

It uses the [Elasticsearch Spark Connector](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html) for reading data from Elasticsearch Index.

Depending upon the versions of Elasticsearch, PySpark and Scala in the environment the Elasticsearch Spark JAR can be downloaded from this [link](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-30). 

The template can support the Elasticsearch versions >= 7.12.0, using the appropriate Elasticsearch Spark Connector

Spark Bigtable connector dependencies are required to be passed when submitting the job.
These dependencies need to be passed by using the --jars flag, or, in the case of Dataproc Templates, using the JARS environment variable.
Some dependencies (jars) must be downloaded from [MVN Repository](https://mvnrepository.com/) and stored your Cloud Storage bucket (create one to store the dependencies).

- **[Spark BigTable Connector](https://cloud.google.com/bigtable/docs/use-bigtable-spark-connector)**
   - gs://spark-lib/bigtable/spark-bigtable_2.12-0.1.0.jar

It also requires [DeltaIO dependencies](https://docs.delta.io/latest/releases.html) to be available in the Dataproc cluster if using delta format.


## Arguments
- `es.bt.input.node`: Elasticsearch Node Uri (format: mynode:9600)
- `es.bt.input.index`: Elasticsearch Input Index Name (format: <index>/<type>)
- `es.bt.input.user`: Elasticsearch Username
- `es.bt.input.password`: Elasticsearch Password
- `es.bt.input.api.key`: API Key for Elasticsearch Authorization 
- `spark.bigtable.project.id`: GCP project where BigTable instance is running
- `spark.bigtable.instance.id`: BigTable instance id
- `es.bt.catalog.json`: BigTable catalog inline json
#### Optional Arguments
- `es.bt.input.es.nodes.path.prefix`: Prefix to add to all requests made to Elasticsearch
- `es.bt.input.es.query`: Holds the query used for reading data from the specified Index
- `es.bt.input.es.mapping.date.rich`: Whether to create a rich Date like object for Date fields in Elasticsearch or returned them as primitives (String or long) (default true)
- `es.bt.input.es.read.field.include`: Fields/properties that are parsed and considered when reading the documents from Elasticsearch. By default empty meaning all fields are considered
- `es.bt.input.es.read.field.exclude`: Fields/properties that are discarded when reading the documents from Elasticsearch
- `es.bt.input.es.read.field.as.array.include`: Fields/properties that should be considered as arrays/lists
- `es.bt.input.es.read.field.as.array.exclude`: Fields/properties that should not be considered as arrays/lists
- `es.bt.input.es.read.metadata`: Whether to include the document metadata (such as id and version) in the results or not in the results or not (default false)
- `es.bt.input.es.read.metadata.field`: The field under which the metadata information is placed (default _metadata)
- `es.bt.input.es.read.metadata.version`: Whether to include the document version in the returned metadata (default false)
- `es.bt.input.es.index.read.missing.as.empty`: Whether elasticsearch-hadoop will allow reading of non existing indices (default no)
- `es.bt.input.es.field.read.empty.as.null`: Whether elasticsearch-hadoop will treat empty fields as null (default yes)
- `es.bt.input.es.read.shard.preference`: The value to use for the shard preference of a search operation when executing a scroll query
- `es.bt.input.es.read.source.filter`: Comma delimited string of field names that you would like to return from Elasticsearch
- `es.bt.input.es.index.read.allow.red.status`: Fetch the data from the available shards and ignore the shards which are not reachable (default false)
- `es.bt.input.es.input.max.docs.per.partition`: The maximum number of documents per input partition. This property is a suggestion, not a guarantee
- `es.bt.input.es.nodes.discovery`: Whether to discover the nodes within the Elasticsearch cluster or only to use the ones given in es.nodes for metadata queries (default true)
- `es.bt.input.es.nodes.client.only`: Whether to use Elasticsearch client nodes (or load-balancers) (default false)
- `es.bt.input.es.nodes.data.only`: Whether to use Elasticsearch data nodes only (default true)
- `es.bt.input.es.nodes.wan.only`: Whether the connector is used against an Elasticsearch instance in a cloud/restricted environment over the WAN, such as Amazon Web Services, in order to use this option set es.bt.input.es.nodes.discovery and es.bt.input.es.nodes.data.only to false (default false)
- `es.bt.input.es.http.timeout`: Timeout for HTTP/REST connections to Elasticsearch (default 1m)
- `es.bt.input.es.http.retries`: Number of retries for establishing a (broken) http connection (default 3)
- `es.bt.input.es.scroll.keepalive`: The maximum duration of result scrolls between query requests (default 10m)
- `es.bt.input.es.scroll.size`: Number of results/items/documents returned per scroll request on each executor/worker/task (default 1000)
- `es.bt.input.es.scroll.limit`: Number of total results/items returned by each individual scroll. A negative value indicates that all documents that match should be returned (default -1)
- `es.bt.input.es.action.heart.beat.lead`: The lead to task timeout before elasticsearch-hadoop informs Hadoop the task is still running to prevent task restart (default 15s)
- `es.bt.input.es.net.ssl`: Enable SSL (default false)
- `es.bt.input.es.net.ssl.cert.allow.self.signed`: Whether or not to allow self signed certificates (default false)
- `es.bt.input.es.net.ssl.protocol`: SSL protocol to be used (default TLS)
- `es.bt.input.es.net.proxy.http.host`: Http proxy host name
- `es.bt.input.es.net.proxy.http.port`: Http proxy port
- `es.bt.input.es.net.proxy.http.user`: Http proxy user name
- `es.bt.input.es.net.proxy.http.pass`: Http proxy password
- `es.bt.input.es.net.proxy.http.use.system.props`: Whether use the system Http proxy properties (namely http.proxyHost and http.proxyPort) or not (default yes)
- `es.bt.input.es.net.proxy.https.host`: Https proxy host name
- `es.bt.input.es.net.proxy.https.port`: Https proxy port
- `es.bt.input.es.net.proxy.https.user`: Https proxy user name
- `es.bt.input.es.net.proxy.https.pass`: Https proxy password
- `es.bt.input.es.net.proxy.https.use.system.props`: Whether use the system Https proxy properties (namely https.proxyHost and https.proxyPort) or not (default yes)
- `es.bt.input.es.net.proxy.socks.host`: Http proxy host name
- `es.bt.input.es.net.proxy.socks.port`: Http proxy port
- `es.bt.input.es.net.proxy.socks.user`: Http proxy user name
- `es.bt.input.es.net.proxy.socks.pass`: Http proxy password
- `es.bt.input.es.net.proxy.socks.use.system.props`: Whether use the system Socks proxy properties (namely socksProxyHost and socksProxyHost) or not (default yes)
- `es.bt.flatten.struct.fields`: Flatten the struct fields
- `es.bt.flatten.array.fields`: Flatten the n-D array fields to 1-D array fields, it needs es.bt.flatten.struct.fields option to be passed
- `spark.bigtable.create.new.table`: Set True if you want to create a BigTable table from catalog. Default value is False means table must be present.
- `spark.bigtable.batch.mutate.size`: BigTable batch mutation size. Maximum allowed value is `100000`. Default is `100`. Rererence [documentation](https://github.com/GoogleCloudDataproc/spark-bigtable-connector/blob/main/spark-bigtable_2.12/src/main/scala/com/google/cloud/spark/bigtable/datasources/BigtableSparkConf.scala#L86)

**Note:** Make sure that either ```es.bt.input.api.key``` or both ```es.bt.input.user``` and ```es.bt.input.password``` is provided. Setting or not setting all three properties at the same time will throw an error.

## Usage

```
$ python main.py --template ELASTICSEARCHTOBIGTABLE --help

usage: main.py [-h]
               --es.bt.input.node ES.BT.INPUT.NODE
               --es.bt.input.index ES.BT.INPUT.INDEX
               --es.bt.input.user ES.BT.INPUT.USER
               --es.bt.input.password ES.BT.INPUT.PASSWORD
               --es.bt.input.api.key ES.BT.INPUT.API.KEY
               --spark.bigtable.project.id ES.BT.PROJECT.ID
               --spark.bigtable.instance.id ES.BT.INSTANCE.ID
               --es.bt.catalog.json ES.BT.CATALOG.JSON
               [--es.bt.input.es.nodes.path.prefix ES.BT.INPUT.ES.NODES.PATH.PREFIX]
               [--es.bt.input.es.query ES.BT.INPUT.ES.QUERY]
               [--es.bt.input.es.mapping.date.rich ES.BT.INPUT.ES.MAPPING.DATE.RICH]
               [--es.bt.input.es.read.field.include ES.BT.INPUT.ES.READ.FIELD.INCLUDE]
               [--es.bt.input.es.read.field.exclude ES.BT.INPUT.ES.READ.FIELD.EXCLUDE]
               [--es.bt.input.es.read.field.as.array.include ES.BT.INPUT.ES.READ.FIELD.AS.ARRAY.INCLUDE]
               [--es.bt.input.es.read.field.as.array.exclude ES.BT.INPUT.ES.READ.FIELD.AS.ARRAY.EXCLUDE]
               [--es.bt.input.es.read.metadata ES.BT.INPUT.ES.READ.METADATA]
               [--es.bt.input.es.read.metadata.field ES.BT.INPUT.ES.READ.METADATA.FIELD]
               [--es.bt.input.es.read.metadata.version ES.BT.INPUT.ES.READ.METADATA.VERSION]
               [--es.bt.input.es.index.read.missing.as.empty ES.BT.INPUT.ES.INDEX.READ.MISSING.AS.EMPTY]
               [--es.bt.input.es.field.read.empty.as.null ES.BT.INPUT.ES.FIELD.READ.EMPTY.AS.NULL]
               [--es.bt.input.es.read.shard.preference ES.BT.INPUT.ES.READ.SHARD.PREFERENCE]
               [--es.bt.input.es.read.source.filter ES.BT.INPUT.ES.READ.SOURCE.FILTER]
               [--es.bt.input.es.index.read.allow.red.status ES.BT.INPUT.ES.INDEX.READ.ALLOW.RED.STATUS]
               [--es.bt.input.es.input.max.docs.per.partition ES.BT.INPUT.ES.INPUT.MAX.DOCS.PER.PARTITION]
               [--es.bt.input.es.nodes.discovery ES.BT.INPUT.ES.NODES.DISCOVERY]
               [--es.bt.input.es.nodes.client.only ES.BT.INPUT.ES.NODES.CLIENT.ONLY]
               [--es.bt.input.es.nodes.data.only ES.BT.INPUT.ES.NODES.DATA.ONLY]
               [--es.bt.input.es.nodes.wan.only ES.BT.INPUT.ES.NODES.WAN.ONLY]
               [--es.bt.input.es.http.timeout ES.BT.INPUT.ES.HTTP.TIMEOUT]
               [--es.bt.input.es.http.retries ES.BT.INPUT.ES.HTTP.RETRIES]
               [--es.bt.input.es.scroll.keepalive ES.BT.INPUT.ES.SCROLL.KEEPALIVE]
               [--es.bt.input.es.scroll.size ES.BT.INPUT.ES.SCROLL.SIZE]
               [--es.bt.input.es.scroll.limit ES.BT.INPUT.ES.SCROLL.LIMIT]
               [--es.bt.input.es.action.heart.beat.lead ES.BT.INPUT.ES.ACTION.HEART.BEAT.LEAD]
               [--es.bt.input.es.net.ssl ES.BT.INPUT.ES.NET.SSL]
               [--es.bt.input.es.net.ssl.cert.allow.self.signed ES.BT.INPUT.ES.NET.SSL.CERT.ALLOW.SELF.SIGNED]
               [--es.bt.input.es.net.ssl.protocol ES.BT.INPUT.ES.NET.SSL.PROTOCOL]
               [--es.bt.input.es.net.proxy.http.host ES.BT.INPUT.ES.NET.PROXY.HTTP.HOST]
               [--es.bt.input.es.net.proxy.http.port ES.BT.INPUT.ES.NET.PROXY.HTTP.PORT]
               [--es.bt.input.es.net.proxy.http.user ES.BT.INPUT.ES.NET.PROXY.HTTP.USER]
               [--es.bt.input.es.net.proxy.http.pass ES.BT.INPUT.ES.NET.PROXY.HTTP.PASS]
               [--es.bt.input.es.net.proxy.http.use.system.props ES.BT.INPUT.ES.NET.PROXY.HTTP.USE.SYSTEM.PROPS]
               [--es.bt.input.es.net.proxy.https.host ES.BT.INPUT.ES.NET.PROXY.HTTPS.HOST]
               [--es.bt.input.es.net.proxy.https.port ES.BT.INPUT.ES.NET.PROXY.HTTPS.PORT]
               [--es.bt.input.es.net.proxy.https.user ES.BT.INPUT.ES.NET.PROXY.HTTPS.USER]
               [--es.bt.input.es.net.proxy.https.pass ES.BT.INPUT.ES.NET.PROXY.HTTPS.PASS]
               [--es.bt.input.es.net.proxy.https.use.system.props ES.BT.INPUT.ES.NET.PROXY.HTTPS.USE.SYSTEM.PROPS]
               [--es.bt.input.es.net.proxy.socks.host ES.BT.INPUT.ES.NET.PROXY.SOCKS.HOST]
               [--es.bt.input.es.net.proxy.socks.port ES.BT.INPUT.ES.NET.PROXY.SOCKS.PORT]
               [--es.bt.input.es.net.proxy.socks.user ES.BT.INPUT.ES.NET.PROXY.SOCKS.USER]
               [--es.bt.input.es.net.proxy.socks.pass ES.BT.INPUT.ES.NET.PROXY.SOCKS.PASS]
               [--es.bt.input.es.net.proxy.socks.use.system.props ES.BT.INPUT.ES.NET.PROXY.SOCKS.USE.SYSTEM.PROPS]
               [--es.bt.flatten.struct.fields]
               [--es.bt.flatten.array.fields]
               [--spark.bigtable.create.new.table ES.BT.CREATE.NEW.TABLE]
               [--spark.bigtable.batch.mutate.size ES.BT.BATCH.MUTATE.SIZE]
               

options:
  -h, --help            show this help message and exit
  --es.bt.input.node ES.BT.INPUT.NODE
                        Elasticsearch Node Uri
  --es.bt.input.index ES.BT.INPUT.INDEX
                        Elasticsearch Input Index Name
  --es.bt.input.user ES.BT.INPUT.USER
                        Elasticsearch Username
  --es.bt.input.password ES.BT.INPUT.PASSWORD
                        Elasticsearch Password   
  --es.bt.input.api.key ES.BT.INPUT.API.KEY
                        API Key for Elasticsearch Authorization
  --es.bt.input.es.nodes.path.prefix ES.BT.INPUT.ES.NODES.PATH.PREFIX
                        Prefix to add to all requests made to Elasticsearch
  --es.bt.input.es.query ES.BT.INPUT.ES.QUERY
                        Holds the query used for reading data from the specified Index
  --es.bt.input.es.mapping.date.rich ES.BT.INPUT.ES.MAPPING.DATE.RICH
                        Whether to create a rich Date like object for Date fields in Elasticsearch or returned them as primitives (String or long)
  --es.bt.input.es.read.field.include ES.BT.INPUT.ES.READ.FIELD.INCLUDE
                        Fields/properties that are parsed and considered when reading the documents from Elasticsearch. By default empty meaning all fields are considered
  --es.bt.input.es.read.field.exclude ES.BT.INPUT.ES.READ.FIELD.EXCLUDE
                        Fields/properties that are discarded when reading the documents from Elasticsearch. By default empty meaning no fields are excluded
  --es.bt.input.es.read.field.as.array.include ES.BT.INPUT.ES.READ.FIELD.AS.ARRAY.INCLUDE
                        Fields/properties that should be considered as arrays/lists
  --es.bt.input.es.read.field.as.array.exclude ES.BT.INPUT.ES.READ.FIELD.AS.ARRAY.EXCLUDE
                        Fields/properties that should not be considered as arrays/lists
  --es.bt.input.es.read.metadata ES.BT.INPUT.ES.READ.METADATA
                        Whether to include the document metadata (such as id and version) in the results or not in the results or not
  --es.bt.input.es.read.metadata.field ES.BT.INPUT.ES.READ.METADATA.FIELD
                        The field under which the metadata information is placed
  --es.bt.input.es.read.metadata.version ES.BT.INPUT.ES.READ.METADATA.VERSION
                        Whether to include the document version in the returned metadata
  --es.bt.input.es.index.read.missing.as.empty ES.BT.INPUT.ES.INDEX.READ.MISSING.AS.EMPTY
                        Whether elasticsearch-hadoop will allow reading of non existing indices
  --es.bt.input.es.field.read.empty.as.null ES.BT.INPUT.ES.FIELD.READ.EMPTY.AS.NULL
                        Whether elasticsearch-hadoop will treat empty fields as null
  --es.bt.input.es.read.shard.preference ES.BT.INPUT.ES.READ.SHARD.PREFERENCE
                        The value to use for the shard preference of a search operation when executing a scroll query
  --es.bt.input.es.read.source.filter ES.BT.INPUT.ES.READ.SOURCE.FILTER
                        Comma delimited string of field names that you would like to return from Elasticsearch
  --es.bt.input.es.index.read.allow.red.status ES.BT.INPUT.ES.INDEX.READ.ALLOW.RED.STATUS
                        Fetch the data from the available shards and ignore the shards which are not reachable
  --es.bt.input.es.input.max.docs.per.partition ES.BT.INPUT.ES.INPUT.MAX.DOCS.PER.PARTITION
                        The maximum number of documents per input partition. This property is a suggestion, not a guarantee
  --es.bt.input.es.nodes.discovery ES.BT.INPUT.ES.NODES.DISCOVERY
                        Whether to discover the nodes within the Elasticsearch cluster or only to use the ones given in es.nodes for metadata queries
  --es.bt.input.es.nodes.client.only ES.BT.INPUT.ES.NODES.CLIENT.ONLY
                        Whether to use Elasticsearch client nodes (or load-balancers)
  --es.bt.input.es.nodes.data.only ES.BT.INPUT.ES.NODES.DATA.ONLY
                        Whether to use Elasticsearch data nodes only
  --es.bt.input.es.nodes.wan.only ES.BT.INPUT.ES.NODES.WAN.ONLY
                        Whether the connector is used against an Elasticsearch instance in a cloud/restricted environment over the WAN, such as Amazon Web Services, in order to use this option set es.bt.input.es.nodes.discovery and es.bt.input.es.nodes.data.only to false
  --es.bt.input.es.http.timeout ES.BT.INPUT.ES.HTTP.TIMEOUT
                        Timeout for HTTP/REST connections to Elasticsearch
  --es.bt.input.es.http.retries ES.BT.INPUT.ES.HTTP.RETRIES
                        Number of retries for establishing a (broken) http connection
  --es.bt.input.es.scroll.keepalive ES.BT.INPUT.ES.SCROLL.KEEPALIVE
                        The maximum duration of result scrolls between query requests
  --es.bt.input.es.scroll.size ES.BT.INPUT.ES.SCROLL.SIZE
                        Number of results/items/documents returned per scroll request on each executor/worker/task
  --es.bt.input.es.scroll.limit ES.BT.INPUT.ES.SCROLL.LIMIT
                        Number of total results/items returned by each individual scroll. A negative value indicates that all documents that match should be returned
  --es.bt.input.es.action.heart.beat.lead ES.BT.INPUT.ES.ACTION.HEART.BEAT.LEAD
                        The lead to task timeout before elasticsearch-hadoop informs Hadoop the task is still running to prevent task restart
  --es.bt.input.es.net.ssl ES.BT.INPUT.ES.NET.SSL
                        Enable SSL
  --es.bt.input.es.net.ssl.cert.allow.self.signed ES.BT.INPUT.ES.NET.SSL.CERT.ALLOW.SELF.SIGNED
                        Whether or not to allow self signed certificates
  --es.bt.input.es.net.ssl.protocol ES.BT.INPUT.ES.NET.SSL.PROTOCOL
                        SSL protocol to be used
  --es.bt.input.es.net.proxy.http.host ES.BT.INPUT.ES.NET.PROXY.HTTP.HOST
                        Http proxy host name
  --es.bt.input.es.net.proxy.http.port ES.BT.INPUT.ES.NET.PROXY.HTTP.PORT
                        Http proxy port
  --es.bt.input.es.net.proxy.http.user ES.BT.INPUT.ES.NET.PROXY.HTTP.USER
                        Http proxy user name
  --es.bt.input.es.net.proxy.http.pass ES.BT.INPUT.ES.NET.PROXY.HTTP.PASS
                        Http proxy password
  --es.bt.input.es.net.proxy.http.use.system.props ES.BT.INPUT.ES.NET.PROXY.HTTP.USE.SYSTEM.PROPS
                        Whether use the system Http proxy properties (namely http.proxyHost and http.proxyPort) or not
  --es.bt.input.es.net.proxy.https.host ES.BT.INPUT.ES.NET.PROXY.HTTPS.HOST
                        Https proxy host name
  --es.bt.input.es.net.proxy.https.port ES.BT.INPUT.ES.NET.PROXY.HTTPS.PORT
                        Https proxy port
  --es.bt.input.es.net.proxy.https.user ES.BT.INPUT.ES.NET.PROXY.HTTPS.USER
                        Https proxy user name
  --es.bt.input.es.net.proxy.https.pass ES.BT.INPUT.ES.NET.PROXY.HTTPS.PASS
                        Https proxy password
  --es.bt.input.es.net.proxy.https.use.system.props ES.BT.INPUT.ES.NET.PROXY.HTTPS.USE.SYSTEM.PROPS
                        Whether use the system Https proxy properties (namely https.proxyHost and https.proxyPort) or not
  --es.bt.input.es.net.proxy.socks.host ES.BT.INPUT.ES.NET.PROXY.SOCKS.HOST
                        Http proxy host name
  --es.bt.input.es.net.proxy.socks.port ES.BT.INPUT.ES.NET.PROXY.SOCKS.PORT
                        Http proxy port
   --es.bt.input.es.net.proxy.socks.user ES.BT.INPUT.ES.NET.PROXY.SOCKS.USER
                        Http proxy user name
  --es.bt.input.es.net.proxy.socks.pass ES.BT.INPUT.ES.NET.PROXY.SOCKS.PASS
                        Http proxy password
  --es.bt.input.es.net.proxy.socks.use.system.props ES.BT.INPUT.ES.NET.PROXY.SOCKS.USE.SYSTEM.PROPS
                        Whether use the system Socks proxy properties (namely socksProxyHost and socksProxyHost) or not     
  --es.bt.flatten.struct.fields
                        Flatten the struct fields
  --es.bt.flatten.array.fields
                        Flatten the n-D array fields to 1-D array fields, it needs es.bt.flatten.struct.fields option to be passed
  --spark.bigtable.project.id ES.BT.PROJECT.ID
                        GCP project id where BigTable instance is running
  --spark.bigtable.instance.id ES.BT.INSTANCE.ID
                        BigTable instance id
  --es.bt.catalog.json ES.BT.CATALOG.JSON
                        BigTable catalog inline json
```

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export JARS="gs://<your_bucket_to_store_dependencies>/elasticsearch-spark-30_2.12-8.11.4.jar,gs://spark-lib/bigtable/spark-bigtable_2.12-0.1.0.jar"
export SPARK_PROPERTIES="spark.jars.packages=org.slf4j:slf4j-reload4j:1.7.36"

./bin/start.sh \
-- --template=ELASTICSEARCHTOBIGTABLE \
   --es.bt.input.node="xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243" \
   --es.bt.input.index="demo" \
   --es.bt.input.user="demo" \
   --es.bt.input.password="demo" \
   --spark.bigtable.project.id="demo-project" \
   --spark.bigtable.instance.id="bt-instance-id" \
   --es.bt.catalog.json='''{
                        "table":{"name":"my_table"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
```