import threading
from queue import Queue

from app.abstractions import AbstractResultPiblisher
from helpers import LoggerMixin, get_pid_number


class ResultPublisherWorker(threading.Thread, LoggerMixin):
    def __init__(
        self,
        queue_in: Queue,
        result_publisher: AbstractResultPiblisher,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._result_publisher = result_publisher
        self._pid = get_pid_number()
        self._messages_processed = 0
        self.logger.info(f"PID: {self._pid} - ResultPublisherWorker inited")

    def run(self) -> None:
        while True:
            payload = self._queue_in.get()
            if "STOP" in payload:
                break

            try:
                self._result_publisher.publish_result(payload)
            except Exception as e:
                self.logger.exception(
                    f"PID: {self._pid} - Failed while publishing results. "
                    f"Error: {e}"
                )
                continue
            self._messages_processed += len(payload)

        self.logger.info(f"PID: {self._pid} - ResultPublisherWorker stopped")
