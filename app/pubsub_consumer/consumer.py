import os
import typing as t

from google.cloud import pubsub_v1

from app.abstractions import AbstractMessageConsumer, message
from helpers import LoggerMixin, get_pid_number
from config import Config


class MessageConsumer(AbstractMessageConsumer, LoggerMixin):
    """
    TODO: Add message validation here - enforce topic message schema
    """

    def __init__(self):
        self._project_id = os.environ.get("PROJECT_ID") or Config.PROJECT_ID
        self._sub_id = os.environ.get("SUB_ID") or Config.SUBSCRIPTION_ID
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

    def get_messages(self) -> t.List[t.Optional[message]]:
        response = self._subscriber.pull(
            request={
                "subscription": self._subscription_path,
                "max_messages": self._num_messages,
            }
        )
        # The subscriber will time out and return nothing if there're no msg
        if not response:
            return []
        ack_ids, messages = [], []
        for message in response.received_messages:
            ack_ids.append(message.ack_id)
            messages.append(eval(message.message.data))
        # Delete messages from the topic
        self._subscriber.acknowledge(
            request={
                "subscription": self._subscription_path,
                "ack_ids": ack_ids,
            }
        )
        return messages

    def __del__(self):
        print("Closing the subscriber")
        if self._subscriber:
            self._subscriber.close()
