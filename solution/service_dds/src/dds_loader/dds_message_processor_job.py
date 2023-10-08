from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size #100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START2")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            
            self._logger.info(f"{datetime.utcnow()}: Message received") 
            payload = msg['payload'] 

            with payload["restaurant"] as pr:
                self._dds_repository.restaurant_insert(pr["id"], pr["name"])
            with payload["user"] as pu:
                self._dds_repository.user_insert(pu["id"], pu["login"], pu["name"]) 
            self._dds_repository.order_insert(payload["id"], payload["date"], payload["cost"], payload["payment"],payload["status"])
            self._dds_repository.order_user_insert(payload["id"], payload["user"]["id"])

            for prod in range(payload["products"]): 

                self._dds_repository.category_insert(prod["category"])
                self._dds_repository.product_insert(prod["id"],prod["name"])
                self._dds_repository.product_category_insert(prod["id"],prod["category"])
                self._dds_repository.order_product_insert(payload["id"],prod["id"])
                self._dds_repository.product_restaurant_insert(prod["id"],payload["restaurant"]["id"])

                cdm_prd_msg = {
                    "object_id": msg["object_id"],
                    "object_type": "user_prod",
                    "payload": {
                        "user_id": payload["user"]["id"],
                        "product_id": prod["id"],
                        "product_name": prod["name"],
                        "order_cnt": 1
                    }
                }
                self._producer.produce(cdm_prd_msg)
                self._logger.info(f"{datetime.utcnow()}. Message cdm_prd_msg sent")

                cdm_categ_msg = {
                    "object_id": msg["object_id"],
                    "object_type": "user_categ",
                    "payload": {
                        "user_id": payload["user"]["id"],
                        "category_id": self._dds_repository.getCategory_id(prod["category"]),
                        "category_name": prod["category"],
                        "order_cnt": 1
                    }
                }
                self._producer.produce(cdm_categ_msg)
                self._logger.info(f"{datetime.utcnow()}. Message cdm_categ_msg sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH2")