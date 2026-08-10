FROM apache/airflow:2.7.2-python3.10

USER root
# Install OpenJDK-11 for PySpark
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

USER airflow

# Copy requirements and install
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
