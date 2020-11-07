
# rakam-api-collector

Real-time data ingestion engine

## Required Configs
**`stream.source`**: `kafka` or `kinesis`
**`target`**: `s3` or custom implementation

1. Poll data data from the `stream.source`
2. Store it in buffer and and deserialize the data into a internal in-memory columnar format `Page`.
3. Flush the data into the `target` periodically using `middleware.max-flush-duration` config.


### `stream.source`:
```
stream.max-flush-duration=15s
middleware.max-flush-duration=60s
```

#### `stream.source`: kinesis

Consumes data from your Kinesis streams

```
kinesis.stream=
kinesis.max-records-per-batch=
kinesis.consumer-dynamodb-table=
aws.enable-cloudwatch=
aws.kinesis-endpoint=
aws.dynamodb-endpoint=
aws.region=
aws.access-key= ## instance profile is used when not provided
aws.secret-access-key= ## instance profile is used when not provided
```

#### `stream.source`: kafka

Consumes data from your Kafka cluster

```
max.poll.records=
historical.worker=false
kafka.topic=
kafka.offset='earliest' or 'latest'
kafka.group.id=
kafka.session.timeout.ms=
kafka.request.timeout.ms=
kafka.nodes=127.0.0.1,127.0.0.2
kafka.historical-data-topic=
source.data-format=AVRO or JSON
```

### `target`:

#### `target`: S3

Creates compressed GZIP files with JSON content inside your S3 bucket

```
target.aws.region=
target.aws.s3-bucket=
target.access-key= ## instance profile is used when not provided
target.secret-access-key= ## instance profile is used when not provided
target.aws.s3-endpoint=
target.aws.s3-max-data-size=
```

#### Developing Custom Targets

Create the following Java classes:

```java
public class CustomModule  
        extends AbstractConfigurationAwareModule  
{  
  @Override  
  protected void setup(Binder binder)  
 {  
  binder.bind(DatabaseHandler.class).to(CustomDatabaseHandler.class).asEagerSingleton();  
  }  
}
```

```java
public class CustomDatabaseHandler extends DatabaseHandler
{
    @Override
	public List<ColumnMetadata> getColumns(String schema, String table) {
	   // Returns the columns for the table. In Rakam API, `schema` corresponds to `project` and table corresponds to `event type`.
	   // If you use Kinesis, we store the metadata in Mysql so in this case, see the S3DatabaseHandler that makes use of the Rakam API metadata database.
	}
	  
	@Override
	public List<ColumnMetadata> addColumns(String schema, String table, List<ColumnMetadata> columns) {
	   // if your source supports data transformation, update your metadata
	} 
	  
	@Override
	public Inserter insert(String schema, String table, List<ColumnMetadata> columns) {
		// create an `Inserter` that performs the action asyncronously in the background thread
	}
}
```

Add it to `io.rakam.presto.ServiceStarter.TargetConfig.Target` enum class for the system to install your module when `target` is set to your implementation and you're done!

If your enum key in `io.rakam.presto.ServiceStarter.TargetConfig.Target` is `CUSTOM`, start the collector with config `target=custom` and it will be used for target connector.
