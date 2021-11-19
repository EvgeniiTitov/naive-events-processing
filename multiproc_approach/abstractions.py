import abc
import typing as t

from google.cloud import pubsub_v1


message = t.Union[pubsub_v1.subscriber.message.Message, str]
processing_result = t.Any


class AbstractMessageConsumer(abc.ABC):

    @abc.abstractmethod
    def get_messages(self) -> t.List[message]:
        ...


class AbstractMessageProcessor(abc.ABC):

    @abc.abstractmethod
    def process_messages(
            self, msg: t.List[message]
    ) -> t.List[processing_result]:
        ...


class AbstractResultPiblisher(abc.ABC):

    @abc.abstractmethod
    def publish_result(self, res: t.List[processing_result]) -> None:
        ...
