import typing as t

from app.abstractions import (
    AbsResultPiblisher,
    processing_result,
)


class BigTablePublisher(AbsResultPiblisher):
    def __init__(self) -> None:
        pass

    def publish_result(self, res: t.List[processing_result]) -> None:
        print("\n\nRESULT TO PUBLISH:", res)
