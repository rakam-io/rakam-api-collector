# rakam-api-collector

Real-time data ingestion engine

## Required Configs
`stream.source`: `kafka` or `kinesis`
`target`: `s3` or custom implementation

1. Poll data data from the `stream.source`
2. Store it in buffer and and deserialize the data into a internal in-memory columnar format `Page`.
3. Flush the data into the `target` periodically using `middleware.max-flush-duration` config.


# `stream.source`:
```
stream.max-flush-duration=15s
middleware.max-flush-duration=60s
```

### `stream.source`: kinesis

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

### `stream.source`: kafka

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

# `target`:

## `target`: S3

Creates compressed GZIP files with JSON content inside your S3 bucket

```
target.aws.region=
target.aws.s3-bucket=
target.access-key= ## instance profile is used when not provided
target.secret-access-key= ## instance profile is used when not provided
target.aws.s3-endpoint=
target.aws.s3-max-data-size=
```

## Developing Custom Targets


