#!/bin/bash

for VAR in `env`
do
  if [[ $VAR =~ ^RAKAM_ ]]; then
    kafka_name=`echo "$VAR" | sed -r "s/RAKAM_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | awk '{gsub(/__/, "-")};1' | tr _ .`
    env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
    echo "$kafka_name=${!env_var}" >> /var/app/collector/etc/config.properties
  fi
done


/var/app/collector/bin/launcher run