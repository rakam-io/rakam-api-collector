#!/bin/bash

java -jar -Xms1024m -Xmx3072m -XX:+UseG1GC  -XX:+HeapDumpOnOutOfMemoryError rakam-data-collector.jar config.properties server

