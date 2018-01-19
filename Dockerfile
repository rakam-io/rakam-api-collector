FROM artifactory.corp.olacabs.com:5000/ubuntu-java8:14.04.1
RUN \
  apt-get update \
  && apt-get install -y --no-install-recommends software-properties-common \
  && apt-get update \
  && rm -rf /var/lib/apt/lists/*

VOLUME /var/log/rakam_data_collector
VOLUME /var/presto/data
VOLUME /data/presto/var/data

RUN chmod -R 777 /var/log/rakam_data_collector
RUN chmod -R 777 /var/presto/data
RUN useradd -ms /bin/bash rakam

ARG CACHEBUST=1

COPY src/main/resources/config_* /home/rakam/
COPY *.sh /home/rakam/
COPY target/rakam-data-collector.jar /home/rakam

RUN java -version

WORKDIR /home/rakam
RUN chmod +x /home/rakam/start.sh
CMD bash -x /home/rakam/start.sh 2>&1
