FROM spark-base

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
ENV SPARK_SUBMIT_ARGS "--packages io.delta:delta-spark_2.12:3.1.0"

CMD ["/bin/bash", "/submit.sh"]
