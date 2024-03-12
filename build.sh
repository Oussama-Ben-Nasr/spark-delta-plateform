# -- Software Stack Version

SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="2.1.5"

# -- Building the Images

export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

docker build \
  -t cluster-base \
  cluster-base \
  --no-cache

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -t spark-base \
  spark-base \
  --no-cache

docker build \
  -t spark-master \
  spark-master \
  --no-cache

docker build \
  -t spark-history \
  spark-history \
  --no-cache

docker build \
  -t spark-worker \
  spark-worker \
  --no-cache

docker build \
  -t jupyterlab \
  spark-jupyterlab \
  --no-cache

docker build \
  -t oussamabennasr/spark-delta-plateform:0.0.1 \
  spark-application \
  --no-cache
