FROM alpine:3.10

ENV ENABLE_INIT_DAEMON false
ENV INIT_DAEMON_BASE_URI http://identifier/init-daemon
ENV INIT_DAEMON_STEP spark_master_init

ENV BASE_URL=https://archive.apache.org/dist/spark/
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3

COPY wait-for-step.sh /
COPY execute-step.sh /
COPY finish-step.sh /


RUN apk add --no-cache curl bash openjdk8-jre python3 py-pip nss libc6-compat coreutils procps \
      && ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2 \
      && chmod +x *.sh \
      && wget ${BASE_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && cd /

#Give permission to execute scripts
RUN chmod +x /wait-for-step.sh && chmod +x /execute-step.sh && chmod +x /finish-step.sh

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
#ENV PYTHONHASHSEED 1
#
#
########################""
#FROM openjdk:11 AS base
#
## Define ENV variables
#ENV SPARK_VERSION=3.2.0
#ENV HADOOP_VERSION=3.2
#
#RUN apt-get update \
#    && apt-get install -y bash tini libc6 libpam-modules krb5-user libnss3 procps
#
#FROM base AS spark-base
#
## Download and extract Spark
#RUN curl -L https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
#    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
#    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
#    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
#
#COPY entrypoint.sh /opt/spark
#
#RUN chmod a+x /opt/spark/entrypoint.sh
#
#FROM spark-base AS sparkbuilder
#
## Set SPARK_HOME
#ENV SPARK_HOME=/opt/spark
#
## Extend PATH environment variable
#ENV PATH=${PATH}:${SPARK_HOME}/bin
#
## Create the application directory
#RUN mkdir -p /app
#
#FROM sparkbuilder AS spark-with-python
#
#ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
#
#RUN apt-get update -y \
#    && apt-get install -y python3 python3-pip \
#    && pip3 install --upgrade pip setuptools \
#    # Removed the .cache to save space
#    && rm -r /root/.cache && rm -rf /var/cache/apt/*
#
#WORKDIR /app
#
## Add requirements file
#ADD requirements.txt .
## Add application files
#ADD . .
## Install application specific python dependencies
#RUN pip3 install -r requirements.txt
#USER root
#
#ENTRYPOINT [ "/opt/spark/entrypoint.sh" ]
