FROM cluster-base

# -- Layer: Apache Spark

ARG spark_version=3.5.0
ARG hadoop_version=3

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    curl -iL http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.8.0-spark3.0-s_2.12/graphframes-0.8.0-spark3.0-s_2.12.jar \
    --output /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/graphframes.jar && \
    rm spark.tgz && \
    apt-get install -y python3-pip

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# -- Runtime

WORKDIR ${SPARK_HOME}

