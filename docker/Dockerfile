FROM apache/airflow:2.10.3

USER root
RUN apt-get update
# install java for spark (Spark runs on the JVM, so the submitter needs Java)
RUN apt-get install -y --no-install-recommends openjdk-17-jre-headless
RUN apt-get install -y --no-install-recommends procps
RUN apt-get install -y --no-install-recommends curl
RUN apt-get install -y --no-install-recommends ca-certificates
RUN apt-get install -y --no-install-recommends gosu

# deletes downloaded package indexes to reduce image size
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

# Create arch-neutral symlink so JAVA_HOME works on both amd64 and arm64
RUN ln -sf /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

ARG SPARK_VERSION=3.5.3

# downloads the Spark binary archive from Apache
RUN curl -fL --progress-bar "https://www.apache.org/dyn/closer.lua/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz?action=download" -o /tmp/spark.tgz

# extracts the Spark archive into /opt
RUN tar -xzf /tmp/spark.tgz -C /opt

# creates a stable symlink so /opt/spark always points to the Spark install
RUN ln -sf /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

# deletes the downloaded archive after extraction
RUN rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:$PATH

# Download S3A connector JARs so Spark can read from MinIO
RUN curl -fL --progress-bar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fL --progress-bar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Download Iceberg Spark runtime JAR (includes iceberg-aws module with S3FileIO)
RUN curl -fL --progress-bar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar

# Download AWS SDK v2 JARs (required by Iceberg S3FileIO)
RUN curl -fL --progress-bar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-v2-2.20.18.jar && \
    curl -fL --progress-bar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.18/url-connection-client-2.20.18.jar \
    -o /opt/spark/jars/url-connection-client-2.20.18.jar

USER airflow
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.12

# Copy and install the pipelines package with all Airflow dockerfile dependencies
COPY --chown=airflow:root pyproject.toml /opt/airflow/
COPY --chown=airflow:root src/ /opt/airflow/src/
RUN pip install --no-cache-dir -e /opt/airflow/[airflow_dockerfile] \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# Copy spark jobs
COPY --chown=airflow:root spark/ /opt/spark/jobs/
