# make sure required volumes exist
mkdir -p shared-vol/history
mkdir -p shared-vol/logs
mkdir -p shared-vol/.ivy2/cache
mkdir -p shared-vol/.ivy2/jars

# initialize env vars for Docker Compose
export SPARK_WORKER_CORES=2
export SHARED_DIR=`pwd`/shared-vol
