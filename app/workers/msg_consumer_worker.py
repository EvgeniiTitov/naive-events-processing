import queue
import typing as t
import threading
from queue import Queue

from app.abstractions import AbstractMessageConsumer, message
from helpers import LoggerMixin, get_pid_number


response = t.List[t.Optional[message]]


class MessageConsumerWorker(threading.Thread, LoggerMixin):
    def __init__(
        self,
        queue_in: Queue,
        queue_out: Queue,
        consumer: AbstractMessageConsumer,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._queue_out = queue_out
        self._consumer = consumer
        self._pid = get_pid_number()
        self.logger.info(f"PID: {self._pid} - MessageConsumerWorker inited")

    def run(self) -> None:
        while True:
            try:
                payload = self._queue_in.get_nowait()
            except queue.Empty:
                pass
            else:
                if "STOP" in payload:
                    self._queue_out.put("STOP")
                    break

            try:
                messages: response = self._consumer.get_messages()
            except Exception as e:
                self.logger.exception(
                    f"PID: {self._pid} - Failed while receiving messages "
                    f"Error: {e}"
                )
                continue

            if not len(messages):
                continue

            self.logger.info(
                f"PID: {self._pid} - MessageConsumerWorker received "
                f"{len(messages)} messages. Sending further"
            )
            self._queue_out.put(messages)

        # TODO: Consider explicitly deleting the consumer
        self.logger.info(f"PID: {self._pid} - MessageConsumerWorker stopped")
