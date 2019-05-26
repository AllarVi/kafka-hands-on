# Readme

## Python 

pyenv activate big_data_project_2

bin/kafka-topics.sh --create --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1 --topic test

bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic test

## Materials used

https://medium.com/@saabeilin/kafka-hands-on-part-i-development-environment-fc1b70955152

https://medium.com/@saabeilin/kafka-hands-on-part-ii-producing-and-consuming-messages-in-python-44d5416f582e

https://medium.com/@tomaszdudek/yet-another-scalable-apache-airflow-with-docker-example-setup-84775af5c451

## Airflow Setup

pyenv activate airflow_jupyter

### Run Jupyter Notebook

papermill task_1/code.ipynb task_1/output/code_exectuion_1.ipynb -f task_1/params.yaml

#### Build image

docker build . -t task1

#### Run dockerized jupyter notebook

docker run -it -e EXECUTION_ID=444444 task1

#### Copy output from container

docker cp <id_of_container>:/notebook/output/code_execution_444444.ipynb ./



















