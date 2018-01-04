#!/bin/bash

echo "Environment variables are: "

env

if [ -z "$s3_loc" ]; then
    echo "Need to set s3_bucket_name"
    exit 1
fi

s3_loc="${s3_loc}"
echo "Getting from, " $s3_loc
aws s3 cp $s3_loc . --recursive
cp *.properties /home/rakam/