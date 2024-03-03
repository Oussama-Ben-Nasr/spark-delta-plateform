#!/bin/bash

export SPARK_HOME=/spark

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

mkdir -p /spark/spark-hs-logs/

ln -sf /dev/stdout $LOG

mkdir -p /tmp/spark-events

# See https://spark.apache.org/docs/latest/monitoring.html#environment-variables
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events -Dspark.history.ui.port=18081"

/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer >> /spark/spark-hs-logs/spark-hs.out