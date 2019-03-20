import os

import structlog
from kafkian import Consumer
from kafkian.serde.deserialization import AvroDeserializer


logger = structlog.getLogger(__name__)

CONSUMER_CONFIG = {
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'default.topic.config': {
        'auto.offset.reset': 'earliest',
    },
    'group.id': 'notifications'
}


def repr_message(message):
    return {
        'topic': message.topic(),
        'key': message.key(),
        'value': message.value(),
        'value_class': message.value().schema.fullname
    }


def handler():
    consumer = Consumer(
        CONSUMER_CONFIG,
        topics=['location_ingress'],
        key_deserializer=AvroDeserializer(schema_registry_url=os.environ['SCHEMA_REGISTRY_URL']),
        value_deserializer=AvroDeserializer(schema_registry_url=os.environ['SCHEMA_REGISTRY_URL']),
    )

    for message in consumer:
        message_rep = repr_message(message)

        if message.value().schema.fullname == 'locations.LocationReceived':
            # Do handle the message
            logger.info("Handling message", **message_rep)
        else:
            logger.warning("Message not handled", **message_rep)

        consumer.commit()


if __name__ == '__main__':
    try:
        handler()
    except KeyboardInterrupt:
        pass
