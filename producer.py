import random
import uuid

import structlog
from confluent_kafka import avro
from kafkian import Producer
from kafkian.serde.avroserdebase import AvroRecord
from kafkian.serde.serialization import AvroSerializer, AvroStringKeySerializer
from time import sleep

logger = structlog.getLogger(__name__)

value_schema_str = """
{
   "namespace": "locations",
   "name": "LocationReceived",
   "type": "record",
   "fields" : [
     {
       "name" : "deviceId",
       "type" : "string"
     },
     {
       "name" : "latitude",
       "type" : "float"
     },
     {
       "name" : "longitude",
       "type" : "float"
     }
   ]
}
"""


class LocationReceived(AvroRecord):
    _schema = avro.loads(value_schema_str)


SCHEMA_REGISTRY_CONFIG = {
    'KAFKA_BOOTSTRAP_SERVERS': 'localhost:29092',
    'SCHEMA_REGISTRY_URL': 'http://localhost:8081'
}

PRODUCER_CONFIG = {
    'bootstrap.servers': SCHEMA_REGISTRY_CONFIG.get('KAFKA_BOOTSTRAP_SERVERS')
}

producer = Producer(
    PRODUCER_CONFIG,
    key_serializer=AvroStringKeySerializer(SCHEMA_REGISTRY_CONFIG.get('SCHEMA_REGISTRY_URL')),
    value_serializer=AvroSerializer(SCHEMA_REGISTRY_CONFIG.get('SCHEMA_REGISTRY_URL'))
)


def produce_location_received(device_id: str, latitude: float, longitude: float):
    message = LocationReceived(dict(
        deviceId=device_id,
        latitude=latitude,
        longitude=longitude
    ))

    logger.msg("Sending to {} {} message: {}".format(SCHEMA_REGISTRY_CONFIG.get('KAFKA_BOOTSTRAP_SERVERS'),
                                                     SCHEMA_REGISTRY_CONFIG.get('SCHEMA_REGISTRY_URL'),
                                                     message))
    try:
        producer.produce('location_ingress', device_id, message, sync=True)
    except Exception as e:
        logger.exception("Failed to produce LocationReceived event: {}".format(e))


if __name__ == '__main__':
    devices = [str(uuid.uuid4()) for _ in range(3)]
    try:
        while True:
            sleep(5)

            device = random.choice(devices)
            produce_location_received(
                device,
                59.3363 + random.random() / 100,
                18.0262 + random.random() / 100
            )
    except KeyboardInterrupt:
        pass
