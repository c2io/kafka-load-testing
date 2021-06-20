import uuid

import typer
from loguru import logger
from kafka import KafkaProducer


app = typer.Typer()


@app.command()
def produce(msg_count: int, topic: str, servers: str):

    producer = KafkaProducer(bootstrap_servers=servers)
    for index in range(msg_count):
        message = f"{index}: {str(uuid.uuid1())}"
        logger.debug(message)
        producer.send(topic, message.encode())


if __name__ == "__main__":
    app()
