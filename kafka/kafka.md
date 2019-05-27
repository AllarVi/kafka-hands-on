kafka.md

# Kafka

## Tutorials

### Hands-On Part I

https://medium.com/@saabeilin/kafka-hands-on-part-i-development-environment-fc1b70955152

### Hands-On Part II

https://medium.com/@saabeilin/kafka-hands-on-part-ii-producing-and-consuming-messages-in-python-44d5416f582e

## Create verifiable producer

kafka-verifiable-producer \
--broker-list kafka-1:9092 \
--max-messages 1000 \
--repeating-keys 10 \
--topic test.test

## Create verifiable consumer

kafka-verifiable-consumer \
--broker-list kafka-1:9092 \
--topic test.test \
--group-id test-1

## Show topic

kafka-topics \
--zookeeper zookeeper-1:2181 \
--list

## Show topic details

kafka-topics \
--zookeeper zookeeper-1:2181 \
--describe \
--topic test.test

## Run Python producer

### Env setup 

pyenv virtualenvs
pyenv virtualenv 3.7.2 venv372
pyenv activate venv372

pip install kafkian==0.9.0 requests structlog colorama

### Running producer and consumer

export KAFKA_BOOTSTRAP_SERVERS=localhost:29092
export SCHEMA_REGISTRY_URL=http://localhost:8081 

python producer.py
python consumer.py