import os
import random
import uuid
from datetime import datetime

import structlog
from confluent_kafka import avro
from kafkian import Producer
from kafkian.serde.avroserdebase import AvroRecord
from kafkian.serde.serialization import AvroSerializer, AvroStringKeySerializer

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
     },
     {
        "name": "time",
        "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
        }
     }
   ]
}
"""


class LocationReceived(AvroRecord):
    _schema = avro.loads(value_schema_str)


PRODUCER_CONFIG = {
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS']
}

producer = Producer(
    PRODUCER_CONFIG,
    key_serializer=AvroStringKeySerializer(os.environ['SCHEMA_REGISTRY_URL']),
    value_serializer=AvroSerializer(os.environ['SCHEMA_REGISTRY_URL'])
)


def produce_location_received(device_id: str, latitude: float, longitude: float, time: datetime):
    message = LocationReceived(dict(
        deviceId=device_id,
        latitude=latitude,
        longitude=longitude,
        time=int(time.timestamp() * 1000),
    ))

    try:
        producer.produce('location_ingress', device_id, message, sync=True)
    except Exception:
        logger.exception("Failed to produce LocationReceived event")


if __name__ == '__main__':
    devices = [str(uuid.uuid4()) for _ in range(3)]
    try:
        while True:
            device = random.choice(devices)
            produce_location_received(
                device,
                59.3363 + random.random() / 100,
                18.0262 + random.random() / 100,
                datetime.now()
            )
    except KeyboardInterrupt:
        pass
