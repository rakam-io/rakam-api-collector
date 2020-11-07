[![Wercker](https://img.shields.io/wercker/ci/wercker/docs.svg)]()

# rakam-presto-collector

## Configs
`target`: `s3` or custom implementation
`stream.source`: `kafka` or `kinesis`

1. Poll data data from the `stream.source`
2. Store it in buffer and and deserialize the data into a internal in-memory columnar format `Page`.
3. Flush the data into the `target` periodically using `middleware.max-flush-duration` config.


# `stream.source`:
```
stream.max-flush-duration=15s
middleware.max-flush-duration=60s
```

### `stream.source`: kinesis
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
```
target.aws.region=
target.aws.s3-bucket=
target.access-key=
target.secret-access-key=
target.aws.s3-endpoint=
target.aws.s3-max-data-size=
```
