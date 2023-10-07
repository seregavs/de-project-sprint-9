from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

from uuid import UUID

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size #100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START3")

        self._logger.info(f"{datetime.utcnow()}: FINISH3")
