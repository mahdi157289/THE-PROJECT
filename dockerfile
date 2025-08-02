FROM bitnami/spark:3.4.0

# Install Java 11 explicitly
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    update-alternatives --set java /usr/lib/jvm/java-11-openjdk-amd64/bin/java

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/bitnami/spark \
    IVY_PATH=/opt/bitnami/spark/.ivy2 \
    SPARK_LOCAL_DIRS=/tmp/spark-temp \
    PATH=$JAVA_HOME/bin:$PATH

# Create directories with correct permissions
RUN mkdir -p ${IVY_PATH} && \
    chown -R 1001:root ${IVY_PATH} && \
    mkdir -p ${SPARK_LOCAL_DIRS} && \
    chown -R 1001:root ${SPARK_LOCAL_DIRS}

# Install additional tools
RUN apt-get install -y wget procps

USER 1001