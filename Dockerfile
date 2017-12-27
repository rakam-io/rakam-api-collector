FROM artifactory.corp.olacabs.com:5000/ubuntu-ola:14.04

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
  #&& apt-get install -y --no-install-recommends oracle-java8-installer \
  && apt-get install -y  python-pip=1.5.4-1 \
  && apt-get install -y maven \
  && pip install awscli==1.10.18 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

VOLUME /var/log/rakam_data_collector
VOLUME /var/presto/data
VOLUME /data/presto/var/data

RUN chmod -R 777 /var/log/rakam_data_collector
RUN chmod -R 777 /var/presto/data
RUN useradd -ms /bin/bash rakam

ARG CACHEBUST=1

COPY src/main/resources/config_* /home/rakam/
COPY jdk-8u151-linux-x64.tar.gz /usr/lib/jvm/
COPY *.sh /home/rakam/
COPY target/rakam-data-collector.jar /home/rakam

WORKDIR /usr/lib/jvm/
RUN tar -xvzf /usr/lib/jvm/jdk-8u151-linux-x64.tar.gz
RUN ls -la /usr/lib/jvm/
RUN sudo update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_151/bin/java" 0
RUN sudo update-alternatives --install "/usr/bin/javac" "javac" "/usr/lib/jvm/jdk1.8.0_151/bin/javac" 0
RUN sudo update-alternatives --set java /usr/lib/jvm/jdk1.8.0_151/bin/java
RUN sudo update-alternatives --set javac /usr/lib/jvm/jdk1.8.0_151/bin/javac

WORKDIR /home/rakam

RUN chmod +x /home/rakam/start.sh
CMD bash -x /home/rakam/start.sh 2>&1




