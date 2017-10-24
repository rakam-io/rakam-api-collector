=======
# rakam-presto-collector

1. Poll data data from Kinesis
2. Store it in buffer and flush the buffer periodically
3. Deserialized the Avro data into Presto internal in-memory columnar format `Page`
4. Create `ORC` file from `Page` using Raptor
5. Calculate the indexes for Raptor, and write the file to local disk and update metadata.
6. Create backup S3 file.

## Sample Config
```
#aws.access-key=
#aws.secret-access-key=

# ----- For Kafka
#stream.source=kafka
#kafka.nodes=127.0.0.1
#zookeeper.nodes=127.0.0.1
#kafka.topic=events
#source.data-format=JSON

# ----- For Kinesis
#stream.source=kinesis
#kinesis.stream=
#kinesis.consumer-dynamodb-table=
#aws.s3-bulk-bucket=
#aws.region=us-east-1

#raptor.aws.region=us-east-1
#raptor.aws.s3-bucket=
#raptor.metadata.url=jdbc:mysql://HOST/DATABASE?user=USERNAME&password=PASSWORD
#raptor.node.id=PRESTO_NODEID
#raptor.storage.data-directory=
```
