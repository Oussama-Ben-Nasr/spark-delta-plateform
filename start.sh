# make sure required volumes exist
mkdir -p shared-vol/history
mkdir -p shared-vol/logs

# initialize env vars for Docker Compose
export SPARK_WORKER_CORES=2
export SHARED_DIR=`pwd`/shared-vol

docker compose up -d