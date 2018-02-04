FROM maven:3.5.2-jdk-8-alpine
MAINTAINER Burak Emre Kabakci "emre@rakam.io"

WORKDIR /var/app

COPY . /var/app/

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

RUN cd /var/app && mvn clean install -DskipTests && tar -xvzf target/collector-*-bundle.tar.gz && mv collector-* collector && touch collector/etc/config.properties

RUN echo "http://dl-8.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories
RUN apk --no-cache --update-cache add python python-dev

ENTRYPOINT [ "/var/app/entrypoint.sh" ]