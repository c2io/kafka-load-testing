import uuid

from locust import task, between
from loguru import logger

from users import KafkaProducerUser


TOPICS = ["hello", "world"]


class HelloWorld(KafkaProducerUser):

    wait_time = between(1, 2.5)

    def on_start(self):
        self.topic = TOPICS.pop()

    @task
    def produce(self):
        message = str(uuid.uuid1())
        logger.debug(f"{self.topic=},{message=}")
        self.send(self.topic, message=message)
