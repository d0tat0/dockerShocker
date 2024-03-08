FROM apache/airflow:latest

ARG MYSQL_USER
ARG MYSQL_PASSWORD
ARG MYSQL_HOST
ARG MYSQL_PORT=3306

ARG MYSQL_BACKEND="mysql+pymysql://${MYSQL_USER}:${MYSQL_PASSWORD}@${MYSQL_HOST}:${MYSQL_PORT}/airflow"

USER root

ENV TZ Etc/UTC

# Update latest security updates and install required dependencies
RUN apt-get update --fix-missing \
    && apt-get -y install sudo systemd cmake curl git wget telnet nginx lsb-release build-essential autoconf libtool pkg-config libssl-dev supervisor nano vim net-tools \
    && apt-get install -y tzdata \
    && echo $TZ > /etc/timezone \
    && apt-get install -y --no-install-recommends vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


USER airflow

# Copy requirements and test DAG
COPY --chown=airflow:root requirements.txt test_dag.py /opt/airflow/

# Install requirements
RUN pip install -r /opt/airflow/requirements.txt

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=True \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${MYSQL_BACKEND} \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor

