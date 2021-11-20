import threading
from queue import Queue

from app.abstractions import AbsMessageProcessor
from app.helpers import LoggerMixin, get_pid_number


class MessageProcessorWorker(threading.Thread, LoggerMixin):
    """
    The worker can use any message processor provided that the processor
    implements the appropriate interface. It is expected that message pre
    processing and postprocessing is implemented within the message processor

    # TODO: Consider adding message validator, which could be passed alongside
            the classifier object to filter out message of incorrect type
    """

    def __init__(
        self,
        queue_in: Queue,
        queue_out: Queue,
        classifier: AbsMessageProcessor,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._queue_out = queue_out
        self._classifier = classifier
        self._pid = get_pid_number()
        self._my_name = self.__class__.__name__
        self.logger.info(f"PID: {self._pid} - {self._my_name} inited")

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
                    f"PID: {self._pid} - {self._my_name} failed while "
                    f"processing messages {payload}. Error: {e}"
                )
                continue

            if not len(predictions):
                self.logger.warning(
                    f"PID: {self._pid} - {self._my_name} got empty "
                    f"predictions from the classifier"
                )
                continue
            self._queue_out.put(predictions)

        self.logger.info(f"PID: {self._pid} - {self._my_name} stopped")
