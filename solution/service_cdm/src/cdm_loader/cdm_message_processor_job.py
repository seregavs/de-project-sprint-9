from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

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
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            
            self._logger.info(f"{datetime.utcnow()}: Message received") 
            payload = msg['payload'] 
            cdm = msg["object_type"]
            if cdm == "user_prod":
               self._cdm_repository.user_product_insert(payload["user_id"], \
                                                        payload["product_id"], \
                                                        payload["product_name"], \
                                                        payload["order_cnt"] )
            elif cdm == "user_categ":
               self._cdm_repository.user_category_insert(payload["user_id"], \
                                                         payload["category_id"], \
                                                         payload["category_name"], \
                                                         payload["order_cnt"] )

        self._logger.info(f"{datetime.utcnow()}: FINISH3")
