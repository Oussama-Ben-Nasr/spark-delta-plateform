#!/bin/bash

export SPARK_MASTER_URL=spark://${SPARK_MASTER_NAME}:${SPARK_MASTER_PORT}
export SPARK_HOME=/usr/bin/spark-3.5.0-bin-hadoop3

if [ ! -z "${SPARK_APPLICATION_PYTHON_LOCATION}" ]; then
    echo "Submit application ${SPARK_APPLICATION_PYTHON_LOCATION} to Spark master ${SPARK_MASTER_URL}"
    echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
    PYSPARK_PYTHON=python3  ${SPARK_HOME}/bin/spark-submit \
        --master ${SPARK_MASTER_URL} \
        ${SPARK_SUBMIT_ARGS} \
        ${SPARK_APPLICATION_PYTHON_LOCATION} ${SPARK_APPLICATION_ARGS}
else
    echo "Not recognized application."
fi

