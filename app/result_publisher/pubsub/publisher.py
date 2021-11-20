import typing as t
import os
from concurrent import futures

from google.cloud import pubsub_v1

from app.abstractions import AbsResultPiblisher, processing_result
from app.helpers import LoggerMixin, get_pid_number
from config import Config


NUM_MESSAGES = os.environ.get("NUM_MESSAGES") or Config.PUBLISH_NUM_MESSAGES


class PubSubMessagePublisher(AbsResultPiblisher, LoggerMixin):

    BATCH_SETTINGS = pubsub_v1.types.BatchSettings(
        max_messages=NUM_MESSAGES,
        max_bytes=1024,
        max_latency=1
    )

    def __init__(self) -> None:
        self._project_id = os.environ.get("PROJECT_ID") or Config.PROJECT_ID
        self._topic_id = (
                os.environ.get("PUBLISH_TOPIC_ID") or Config.PUBLISH_TOPIC_ID
        )
        self._num_messages = NUM_MESSAGES
        self._pid = get_pid_number()
        self._publisher = pubsub_v1.PublisherClient(
            PubSubMessagePublisher.BATCH_SETTINGS
        )
        self._topic_path = self._publisher.topic_path(
            self._project_id, self._topic_id
        )
        self._my_name = self.__class__.__name__
        self.logger.info(f"PID: {self._pid} - {self._my_name} inited")

    def publish_result(self, res: t.List[processing_result]) -> None:
        if not len(res):
            return
        publish_futures = []
        for result in res:
            payload = str({
                "crn": result[0],
                "prediction": result[-1],
                "features": result[1]
            }).encode("utf-8")
            publish_future = self._publisher.publish(
                self._topic_path, payload
            )
            publish_futures.append(publish_future)

        futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
        self.logger.debug(
            f"PID: {self._pid} - {self._my_name} published {len(res)} messages"
        )