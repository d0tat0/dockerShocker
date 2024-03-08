FROM apache/airflow:latest

USER root

ARG MYSQL_BACKEND
USER root


# Set timezone
ENV TZ Etc/UTC
RUN apt-get install -y tzdata
RUN echo $TZ > /etc/timezone

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY . ./
USER airflow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$MYSQL_BACKEND

USER root

CMD [ "/sbin/init" ]