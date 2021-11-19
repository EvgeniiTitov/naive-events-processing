import threading
from queue import Queue

from multiproc_approach.abstractions import AbstractMessageProcessor
from helpers import LoggerMixin, get_pid_number


class MessageProcessorWorker(threading.Thread, LoggerMixin):
    def __init__(
        self,
        queue_in: Queue,
        queue_out: Queue,
        classifier: AbstractMessageProcessor,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._queue_out = queue_out
        self._classifier = classifier
        self._pid = get_pid_number()
        self.logger.info(f"PID: {self._pid} - MessageProcessorWorker inited")

    def run(self) -> None:
        while True:
            payload = self._queue_in.get()
            if "STOP" in payload:
                self._queue_out.put("STOP")
                break

            try:
                predictions = self._classifier.process_messages(payload)
            except Exception as e:
                self.logger.exception(
                    f"PID: {self._pid} - Failed while processing messages. "
                    f"Error: {e}"
                )
                continue

            if not len(predictions):
                self.logger.warning(
                    f"PID: {self._pid} - MessageProcessorWorker got empty "
                    f"predictions from the classifier"
                )
                continue
            self._queue_out.put(predictions)

        self.logger.info(f"PID: {self._pid} - MessageProcessorWorker stopped")
