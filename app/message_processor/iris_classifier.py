import os
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
        self._model = pickle.load(
            open(
                os.path.join(
                    os.path.dirname(__file__), "weights/iris_classifier.pkl"
                ),
                "rb",
            )
        )
        self._pid = get_pid_number()
        self.logger.info(f"IrisClassifier loaded in PID: {self._pid}")

    def process_messages(
        self, msg: t.List[message]
    ) -> t.List[processing_result]:
        crns, batch = [], []
        for message in msg:
            crns.append(message["crn"])
            batch.append(message["features"])

        prediction_indices = self._model.predict(batch)

        out = []
        for crn, features, index in zip(crns, batch, prediction_indices):
            out.append([crn, features, IrisClassifier.CLASSES[index]])
        return out
