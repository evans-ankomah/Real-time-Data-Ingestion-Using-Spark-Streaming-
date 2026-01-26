# Custom Spark image with Python dependencies and PostgreSQL JDBC driver
FROM apache/spark:3.5.0-python3

# Switch to root to install packages
USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    faker==24.0.0 \
    psycopg2-binary==2.9.9

# Download PostgreSQL JDBC driver
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.7.1.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Create app directories
RUN mkdir -p /app/src /app/data/raw /app/data/processed /app/data/error /app/checkpoints /app/config

# Set working directory
WORKDIR /app

# Copy source files
COPY src/ /app/src/
COPY config/ /app/config/

# Set proper permissions
RUN chmod -R 755 /app

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# Default command - keep container running
CMD ["tail", "-f", "/dev/null"]
