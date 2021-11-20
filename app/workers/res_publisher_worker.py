import threading
from queue import Queue

from app.abstractions import AbsResultPiblisher
from app.helpers import LoggerMixin, get_pid_number


class ResultPublisherWorker(threading.Thread, LoggerMixin):
    """
    The worker can use any result publisher provided that the publisher
    implements the appropriate interface.
    """

    def __init__(
        self,
        queue_in: Queue,
        result_publisher: AbsResultPiblisher,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._result_publisher = result_publisher
        self._pid = get_pid_number()
        self._messages_processed = 0
        self._my_name = self.__class__.__name__
        self.logger.info(f"PID: {self._pid} - {self._my_name} inited")

    @property
    def messages_processed(self) -> int:
        return self._messages_processed

    def run(self) -> None:
        while True:
            payload = self._queue_in.get()
            if "STOP" in payload:
                break

            try:
                self._result_publisher.publish_result(payload)
            except Exception as e:
                self.logger.exception(
                    f"PID: {self._pid} - {self._my_name} failed while "
                    f"publishing results {payload}. Error: {e}"
                )
                continue
            self._messages_processed += len(payload)

        self.logger.info(f"PID: {self._pid} - {self._my_name} stopped")
