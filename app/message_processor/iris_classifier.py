import pickle
import typing as t

from helpers import get_pid_number, LoggerMixin
from app.abstractions import (
    AbstractMessageProcessor,
    message,
    processing_result,
)


class IrisClassifier(AbstractMessageProcessor, LoggerMixin):
    CLASSES = ["setosa", "versicolor", "virginica"]

    def __init__(self) -> None:
        self._model = pickle.load(open("weights/iris_classifier.pkl", "rb"))
        self._pid = get_pid_number()
        self.logger.info(f"IrisClassifier loaded in PID: {self._pid}")

    def process_messages(
        self, msg: t.List[message]
    ) -> t.List[processing_result]:
        # TODO: For a normal model data needs to be preprocessed

        predictions = self._model.predict(msg)

        # TODO: Predictions must potentially be postprocessed
        return [IrisClassifier.CLASSES[index] for index in predictions]
