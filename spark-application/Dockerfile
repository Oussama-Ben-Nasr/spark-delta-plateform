FROM spark-base

COPY wait-for-step.sh /
COPY execute-step.sh /
COPY finish-step.sh /

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077

COPY submit.sh /

# Copy the requirements.txt first, for separate dependency resolving and downloading
COPY requirements.txt /app/
WORKDIR /app
RUN pip3 install -r requirements.txt

# Copy the source code
COPY . /app

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_ARGS ""

CMD ["/bin/bash", "/submit.sh"]
