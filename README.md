# rakam-presto-collector

1. Poll data data from Kinesis
2. Store it in buffer and flush the buffer periodically
3. Deserialized the Avro data into Presto internal in-memory columnar format `Page`
4. Create `ORC` file from `Page` using Raptor
5. Calculate the indexes for Raptor, and write the file to local disk and update metadata.
6. Create backup S3 file.
