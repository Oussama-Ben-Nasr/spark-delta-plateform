# -- Software Stack Version

SPARK_VERSION="3.0.0"
HADOOP_VERSION="2.7"
JUPYTERLAB_VERSION="2.1.5"

# -- Building the Images

export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

docker build \
  -t cluster-base \
  cluster-base

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -t spark-base \
  spark-base

docker build \
  -t spark-master \
  spark-master

docker build \
  -t spark-history \
  spark-history

docker build \
  -t spark-worker \
  spark-worker

docker build \
  -t jupyterlab \
  spark-jupyterlab

