import typing as t

from app.abstractions import (
    AbstractResultPiblisher,
    processing_result,
)


class BigTablePublisher(AbstractResultPiblisher):
    def __init__(self) -> None:
        pass

    def publish_result(self, res: t.List[processing_result]) -> None:
        pass
