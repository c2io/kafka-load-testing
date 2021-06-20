import time

from kafka import KafkaProducer
from locust import events, User
from locust.exception import LocustError
from loguru import logger


class KafkaProducerUser(User):

    abstract = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.host is None:
            raise LocustError(
                "You must specify the base host. Either in the host attribute in the User class, or on the command line using the --host option."
            )

        self.producer = KafkaProducer(bootstrap_servers=self.host)

    def __handle_success(self, *arguments, **kwargs):
        end_time = time.time()
        elapsed_time = int((end_time - kwargs["start_time"]) * 1000)
        try:
            record_metadata = kwargs["future"].get(timeout=1)

            request_data = dict(
                request_type="ENQUEUE",
                name=record_metadata.topic,
                response_time=elapsed_time,
                response_length=record_metadata.serialized_value_size,
            )

            self.__fire_success(**request_data)
        except Exception as ex:
            logger.debug("Logging the exception : {0}".format(ex))
            raise  # ??

    def __handle_failure(self, *arguments, **kwargs):
        logger.debug("failure " + str(locals()))
        end_time = time.time()
        elapsed_time = int((end_time - kwargs["start_time"]) * 1000)

        request_data = dict(
            request_type="ENQUEUE",
            name=kwargs["topic"],
            response_time=elapsed_time,
            exception=arguments[0],
        )

        self.__fire_failure(**request_data)

    def __fire_failure(self, **kwargs):
        events.request_failure.fire(**kwargs)

    def __fire_success(self, **kwargs):
        events.request_success.fire(**kwargs)

    def send(self, topic, key=None, message=None):
        start_time = time.time()
        future = self.producer.send(
            topic,
            key=key.encode() if key else None,
            value=message.encode() if message else None,
        )
        future.add_callback(self.__handle_success, start_time=start_time, future=future)
        future.add_errback(self.__handle_failure, start_time=start_time, topic=topic)

    def finalize(self):
        logger.debug("flushing the messages")
        self.producer.flush(timeout=5)
        logger.debug("flushing finished")
