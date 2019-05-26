FROM puckel/docker-airflow:1.10.2

USER root

RUN groupadd --gid 999 docker \
    && usermod -aG docker airflow

USER airflow
