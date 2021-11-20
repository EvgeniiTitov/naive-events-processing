import typing as t
from queue import Queue

from app.workers import (
    MessageConsumerWorker,
    MessageProcessorWorker,
    ResultPublisherWorker,
)
from app.message_consumer import PubSubMessageConsumer
from app.message_processor import IrisClassifier
from app.result_publisher import (
    BigTablePublisher,
    PubSubMessagePublisher,
    BigQueryMessagePublisher,
)
from app.helpers import LoggerMixin, get_pid_number
from config import Config


class App(LoggerMixin):
    def __init__(self) -> None:
        self._pid = get_pid_number()
        self._my_name = self.__class__.__name__

        # Initialize queues connecting workers
        self._q_to_consumer: "Queue[str]" = Queue(1)
        self._q_consumer_proc: "Queue[t.Any]" = Queue(
            Config.Q_SIZE_CONSUMER_TO_PROCESSOR
        )
        self._q_proc_publisher: "Queue[t.Any]" = Queue(
            Config.Q_SIZE_PROCESSOR_TO_PUBLISHER
        )
        self.logger.info("Queues initialized")

        # Initialize classes operated by the workers
        self._pubsub_consumer = PubSubMessageConsumer()
        self._model = IrisClassifier()

        # self._res_publisher = BigTablePublisher()
        # self._res_publisher = PubSubMessagePublisher()
        self._res_publisher = BigQueryMessagePublisher()

        # Initialize workers (threads) doing actual work
        self._threads = []
        self._consumer_thread = MessageConsumerWorker(
            queue_in=self._q_to_consumer,
            queue_out=self._q_consumer_proc,
            consumer=self._pubsub_consumer,
        )
        self._threads.append(self._consumer_thread)

        self._processor_thread = MessageProcessorWorker(
            queue_in=self._q_consumer_proc,
            queue_out=self._q_proc_publisher,
            classifier=self._model,
        )
        self._threads.append(self._processor_thread)  # type: ignore

        self._publisher_thread = ResultPublisherWorker(
            queue_in=self._q_proc_publisher,
            result_publisher=self._res_publisher,
        )
        self._threads.append(self._publisher_thread)  # type: ignore
        self.logger.info(f"PID: {self._pid} - {self._my_name} inited")

    def start(self) -> None:
        for thread in self._threads:
            thread.start()
        self.logger.info(f"PID: {self._pid} - {self._my_name} started")

    def stop(self) -> None:
        self._q_to_consumer.put("STOP")
        for thread in self._threads:
            thread.join()
        self.logger.info(f"PID: {self._pid} - {self._my_name} stopped")

    def get_number_of_processed_messages(self) -> int:
        return self._publisher_thread.messages_processed

    def report_queue_sizes(self) -> t.MutableMapping[str, int]:
        return {
            "consumer_to_processor": self._q_consumer_proc.qsize(),
            "processor_to_publisher": self._q_proc_publisher.qsize(),
        }
