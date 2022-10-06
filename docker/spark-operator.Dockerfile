
FROM gcr.io/spark-operator/spark-py:v3.1.1

USER root

COPY ./jars/ $SPARK_HOME/jars

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-cache-dir

RUN mkdir -p /src 

COPY ./scripts/ /src

WORKDIR /src
