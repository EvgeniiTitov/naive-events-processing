import abc
import typing as t


message = t.Union[str, dict]
processing_result = t.Any


class AbsMessageConsumer(abc.ABC):
    @abc.abstractmethod
    def get_messages(self) -> t.List[t.Optional[message]]:
        ...


class AbsMessageProcessor(abc.ABC):
    @abc.abstractmethod
    def process_messages(
        self, messages: t.List[message]
    ) -> t.List[processing_result]:
        ...


class AbsResultPiblisher(abc.ABC):
    @abc.abstractmethod
    def publish_result(self, res: t.List[processing_result]) -> None:
        ...
