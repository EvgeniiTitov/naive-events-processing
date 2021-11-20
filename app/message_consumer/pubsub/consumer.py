import os
import typing as t

from google.cloud import pubsub_v1

from app.abstractions import AbsMessageConsumer, message
from app.helpers import LoggerMixin, get_pid_number
from config import Config


class PubSubMessageConsumer(AbsMessageConsumer, LoggerMixin):
    def __init__(self):
        self._project_id = os.environ.get("PROJECT_ID") or Config.PROJECT_ID
        self._sub_id = (
            os.environ.get("SUB_ID") or Config.CONSUME_SUBSCRIPTION_ID
        )
        self._num_messages = (
            os.environ.get("NUM_MESSAGES") or Config.CONSUME_NUM_MESSAGES
        )
        self._subscriber = pubsub_v1.SubscriberClient()
        self._subscription_path = self._subscriber.subscription_path(
            self._project_id, self._sub_id
        )
        self._pid = get_pid_number()
        self._my_name = self.__class__.__name__
        self.logger.info(f"PID: {self._pid} - {self._my_name} initialized")

    def get_messages(self) -> t.List[t.Optional[message]]:
        response = self._subscriber.pull(
            request={
                "subscription": self._subscription_path,
                "max_messages": self._num_messages,
            }
        )
        # The subscriber will time out and return nothing if there are no msg
        if not response:
            return []
        ack_ids, messages = [], []
        for message in response.received_messages:
            ack_ids.append(message.ack_id)
            messages.append(eval(message.message.data))

        # Delete messages from the topic
        # TODO: Can I do it async?
        self._subscriber.acknowledge(
            request={
                "subscription": self._subscription_path,
                "ack_ids": ack_ids,
            }
        )
        return messages

    def __del__(self):
        if self._subscriber:
            self.logger.debug(
                f"PID: {self._pid} - {self._my_name} closing the subscriber"
            )
            self._subscriber.close()
