[![Wercker](https://img.shields.io/wercker/ci/wercker/docs.svg)]()

# rakam-presto-collector

1. Poll data data from Kinesis or Kafka
2. Store it in buffer and flush the buffer periodically
3. Deserialized the JSON/Avro data into Presto internal in-memory columnar format `Page`
4. Create `ORC` file from `Page` using Raptor
5. Calculate the indexes for Raptor, and write the file to local disk and update metadata.
6. Create backup S3 file.

## Sample Config
```
#aws.access-key=
#aws.secret-access-key=
#aws.s3-bulk-bucket=
#aws.region=us-east-1

# ----- For Kafka
#stream.source=kafka
#kafka.nodes=127.0.0.1
#kafka.topic=presto_test_2
# 'earliest' or 'latest'
#kafka.offset=earliest

#kafka.group.id=

#zookeeper.nodes=127.0.0.1

#database.checkpoint-field=_shard_time

#raptor.aws.region=us-east-1
#raptor.aws.s3-bucket=

#raptor.metadata.url=jdbc:mysql://localhost/presto?user=presto&password=presto
#raptor.node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
#raptor.storage.data-directory=/var/presto/data
#raptor.presto-url=http://localhost:8080/
#log-active=false
#database.user-excluded-columns=_shard_time,_collection,_project