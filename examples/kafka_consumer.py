from kafka import KafkaConsumer
import typer
from loguru import logger

app = typer.Typer()


@app.command()
def consume(topic: str, servers: str):
    consumer = KafkaConsumer(topic, bootstrap_servers=servers)
    for msg in consumer:
        logger.debug(msg)


if __name__ == "__main__":
    app()
