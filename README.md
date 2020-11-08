
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
        extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(DatabaseHandler.class).to(CustomDatabaseHandler.class).asEagerSingleton();
    }
}
```

```java
public class CustomDatabaseHandler extends AbstractDatabaseHandler {
    @Inject
    public CustomDatabaseHandler(@Named("metadata.store.jdbc") JDBCPoolDataSource metadataStore) {
        super(metadataStore);
    }

    @Override
    public Inserter insert(String projectName, String eventType, List<ColumnMetadata> eventProperties) {
        return new Inserter() {
	    // we make use of PrestoSQL's Page as it provides us a way to process the data in a columnar and efficient way. See: https://github.com/prestosql/presto/blob/master/presto-spi/src/main/java/io/prestosql/spi/Page.java
            @Override
            public void addPage(Page page) {
                for (int i = 0; i < eventProperties.size(); i++) {
                    ColumnMetadata property = eventProperties.get(i);
                    Block block = page.getBlock(i);
                    if(property.getType() == DoubleType.DOUBLE) {
                        // the `value` is the first value of property in our micro-batch
                        double value = DoubleType.DOUBLE.getDouble(block, 0);
                    }
                }
            }

            @Override
            public CompletableFuture commit() {
                // if the future is completed, the checkpoint will be executed
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
```

Add it to `io.rakam.presto.ServiceStarter.TargetConfig.Target` enum class for the system to install your module when `target` is set to your implementation and you're done!

If your enum key in `io.rakam.presto.ServiceStarter.TargetConfig.Target` is `CUSTOM`, start the collector with config `target=custom` and it will be used for target connector.
