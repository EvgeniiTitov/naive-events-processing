import queue
import typing as t
import threading
from queue import Queue

from app.abstractions import AbsMessageConsumer, message
from app.helpers import LoggerMixin, get_pid_number


response = t.List[t.Optional[message]]


class MessageConsumerWorker(threading.Thread, LoggerMixin):
    """
    The worker can use any Consumer as long as the provided consumer implements
    the appropriate interface.
    """

    def __init__(
        self,
        queue_in: Queue,
        queue_out: Queue,
        consumer: AbsMessageConsumer,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._queue_out = queue_out
        self._consumer = consumer
        self._pid = get_pid_number()
        self._my_name = self.__class__.__name__
        self.logger.info(f"PID: {self._pid} - {self._my_name} inited")

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
                    f"PID: {self._pid} - {self._my_name} failed while "
                    f"receiving messages Error: {e}"
                )
                continue

            # Consumer can timeout returning no messages
            if not len(messages):
                continue

            self.logger.debug(
                f"PID: {self._pid} - {self._my_name} received "
                f"{len(messages)} messages. Sending further"
            )
            self._queue_out.put(messages)

        # TODO: Consider explicitly deleting the consumer
        self.logger.info(f"PID: {self._pid} - {self._my_name} stopped")
