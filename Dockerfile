FROM artifactory.corp.olacabs.com:5000/ubuntu-ola:14.04
MAINTAINER dataservices <dataservices@olacabs.com>
RUN \
  apt-get update \
  && apt-get install -y --no-install-recommends software-properties-common \
  && add-apt-repository ppa:webupd8team/java \
  && gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 \
  && apt-get update \
  && echo debconf shared/accepted-oracle-license-v1-1 select true |  debconf-set-selections \
  && echo debconf shared/accepted-oracle-license-v1-1 seen true |  debconf-set-selections \
  && apt-get install -y  python-pip=1.5.4-1 \
  && pip install awscli==1.10.18 \
  && apt-get install -y --no-install-recommends oracle-java8-installer \
  && apt-get install -y  python-pip=1.5.4-1 \
  && apt-get install -y maven \
  && pip install awscli==1.10.18 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

VOLUME /var/log/rakam_data_collector
RUN chmod -R 777 /var/log/rakam_data_collector
RUN useradd -ms /bin/bash rakam

COPY * /home/rakam/
COPY src/main/resources/config.properties /home/rakam
CMD ["export JAVA_HOME=/usr/bin/java"]

 WORKDIR /home/rakam

RUN mvn clean install -Dmaven.test.skip=true
COPY target/rakam-data-collector.jar /home/rakam
RUN chmod +x *.sh

CMD ["/home/rakam/start.sh"]




