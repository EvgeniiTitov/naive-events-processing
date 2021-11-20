import os
import typing as t

from google.cloud import bigquery

from app.abstractions import AbsResultPiblisher, processing_result
from app.helpers import LoggerMixin, get_pid_number
from config import Config


class BigQueryMessagePublisher(AbsResultPiblisher, LoggerMixin):
    def __init__(self) -> None:
        self._my_name = self.__class__.__name__
        self._pid = get_pid_number()

        self._client = bigquery.Client()
        self._table_id = os.environ.get("BQ_TABLE") or Config.BQ_TABLE

        self.logger.info(f"PID: {self._pid} - {self._my_name} inited")

    def publish_result(self, res: t.List[processing_result]) -> None:
        rows_to_insert = [
            {
                "crn": result[0],
                "prediction": result[-1],
                "features": str(result[1]),
            }
            for result in res
        ]
        total_rows = len(rows_to_insert)
        errors = self._client.insert_rows_json(self._table_id, rows_to_insert)
        if len(errors):
            self.logger.exception(
                f"PID: {self._pid} - {self._my_name} failed while inserting "
                f"{rows_to_insert} into {self._table_id}. Errors: {errors}"
            )
        else:
            self.logger.info(
                f"PID: {self._pid} - {self._my_name} published {total_rows} "
                f"row to {self._table_id}"
            )
