# Reproducible PySpark environment for the CSC 467 project.
#
# Why this exists: running Spark on Windows needs winutils.exe + hadoop.dll and a
# carefully configured conda env. On Linux (this image) none of that is needed --
# Spark "just works" with a JDK + pip-installed pyspark. So this container makes
# the project run identically on any machine with Docker, with zero Windows hacks.

# Base: small Python 3.11 image (matches the local conda env's Python version).
# Pinned to Debian "bookworm": its default-jdk is JDK 17, which Spark 3.5.1
# officially supports. (Newer Debian "trixie" ships JDK 21, which Spark 3.5.1 does
# NOT officially support, so we pin bookworm for a reproducible, supported JVM.)
FROM python:3.11-slim-bookworm

# Spark is a JVM engine, so it needs Java (JDK 17 here). procps provides `ps`,
# which Spark's startup script calls. No Hadoop/winutils shim is needed on Linux.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# Install Python deps first so this layer is cached unless requirements change.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the code (data is mounted at runtime, NOT baked into the image).
COPY src/ ./src/
COPY verify_spark.py .

# Default command just verifies Spark starts; real scripts are run via
# `docker compose run` (see README).
CMD ["python", "verify_spark.py"]
