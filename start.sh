#!/bin/bash

#source setup.sh

echo kafka.topic=${KAFKA_TOPIC} >> /home/rakam/config_${SERVICE_ENV}.properties
echo kafka.offset=${KAFKA_OFFSET} >> /home/rakam/config_${SERVICE_ENV}.properties
echo kafka.group.id=${KAFKA_GROUP_ID} >> /home/rakam/config_${SERVICE_ENV}.properties
echo database.whitelisted.collections=${WHITELIST_COLLECTIONS} >> /home/rakam/config_${SERVICE_ENV}.properties

java -jar -Xms${JAVA_PROCESS_MIN_HEAP} -Xmx${JAVA_PROCESS_MAX_HEAP} -XX:+${GC_ALGO}  -XX:+HeapDumpOnOutOfMemoryError /home/rakam/rakam-data-collector.jar /home/rakam/config_${SERVICE_ENV}.properties server
