from loguru import logger
from locust import events, task, User, between
import arrow

users = [1, 2, 3, 4]


class HelloUser(User):
    """
    poetry run python -m locust \
        --locustfile examples/locustfile.py \
        --headless \
        --users 4 \
        --spawn-rate 4 \
        --run-time 10s \
        --reset-stats --only-summary
    """

    wait_time = between(1, 2.5)

    # 初始化不同的user
    def on_start(self):
        self.user = users.pop()
        logger.info(f"{self.user=}")

    @task
    def hello_world(self):
        start_time = arrow.now()
        logger.debug(f"{self.user=}")
        end_time = arrow.now()

        # 耗时+成功失败
        total_cost_ms = (end_time - start_time).microseconds
        events.request_success.fire(
            request_type="HelloUser",
            name="hello_world",
            response_time=total_cost_ms,
            response_length=0,
        )
