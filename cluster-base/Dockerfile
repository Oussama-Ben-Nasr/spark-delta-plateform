ARG debian_buster_image_tag=17-jdk-slim
FROM openjdk:${debian_buster_image_tag}

# -- Layer: OS + Python 3.7

ARG shared_workspace=/opt/workspace
ARG spark_version=3.5.0

COPY requirements.txt /usr/bin/requirements.txt

RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
    apt-get install -y procps && \
    apt-get install -y python3 && \
    apt-get install -y wget && \
    apt-get install -y curl && \
    apt-get install -y less && \
    apt-get install -y unzip && \
    apt-get install -y python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip && \
    pip3 install -r /usr/bin/requirements.txt

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]
