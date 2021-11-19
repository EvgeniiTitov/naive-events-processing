import os
import typing as t

from google.cloud import pubsub_v1

from app.abstractions import AbstractMessageConsumer, message
from helpers import LoggerMixin, get_pid_number
from config import Config


"""
Either 1 or multiple messages could be received
Post process messages so just raw messages (strings) passed further
"""


class MessageConsumer(AbstractMessageConsumer, LoggerMixin):
    """
    TODO: Add message validation here
    """

    def __init__(self):
        self._project_id = os.environ.get("PROJECT_ID") or Config.PROJECT_ID
        self._sub_id = os.environ.get("SUBSCRIPTION_ID") or Config.SUB_ID

        self._num_messages = (
            os.environ.get("NUM_MESSAGES") or Config.NUM_MESSAGES
        )
        self._subscriber = pubsub_v1.SubscriberClient()
        self._subscription_path = self._subscriber.subscription_path(
            self._project_id, self._sub_id
        )
        self.logger.info(
            f"MessageConsumer initialized in PID: {get_pid_number()}"
        )

    def get_messages(self) -> t.List[message]:
        # TODO: Complete and test this
        with self._subscriber:
            response = self._subscriber.pull(
                request={
                    "subscription": self._subscription_path,
                    "max_messages": self._num_messages,
                }
            )
