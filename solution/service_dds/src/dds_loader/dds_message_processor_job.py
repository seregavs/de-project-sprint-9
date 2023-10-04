from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
import uuid

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START2")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            
            self._logger.info(f"{datetime.utcnow()}: Message received") 
            payload = msg['payload'] 
            # upsert данных в кучу таблиц Data Vault
            # ...
            # CREATE TABLE dds.h_category (
            #     h_category_pk uuid NOT NULL,
            #     category_name varchar NOT NULL,
            #     load_dt timestamp NOT NULL,
            #     load_src varchar NOT NULL,
            #     CONSTRAINT h_category_pk PRIMARY KEY (h_category_pk)
            # );

            # class OrderDdsBuilder:
            #     def __init__(self, dict: Dict) -> None:
            #         self._dict = dict
            #         self.source_system = "orders-system-kafka"
            #         self.order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')

            #     def _uuid(self, obj: Any) -> uuid.UUID:
            #         return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj)) 
            # ```

            # вот, 7f288a2e-0ad0-4039-8e59-6c9838d87307 - просто рандомная строка 
            # #$# - ну тоже просто важном, чтобы сгенерить uuid

            # hk_order_product_pk=self._uuid(f"{order_id}#$#{prod_id}"),
            
            # формирование сообщений для CDM-витрины
            for prod in range(payload["products"]): 
                cdm_prd_msg = {
                    "object_id": 12121, # get unique value?
                    "object_type": "user_prod",
                    "payload": {
                        "user_id": payload["user"]["id"],
                        "product_id": prod["id"],
                        "product_name": prod["name"],
                        "order_cnt": 1
                    }
                }
                self._producer.produce(cdm_prd_msg)
                self._logger.info(f"{datetime.utcnow()}. Message cdm_prd_msg Sent")

                cdm_categ_msg = {
                    "object_id": 12121, # get unique value?
                    "object_type": "user_categ",
                    "payload": {
                        "user_id": payload["user"]["id"],
                        "category_id": 1, # need to get h_category_pk from table dds.h_category
                        "category_name": prod["category"],
                        "order_cnt": 1
                    }
                }
                self._producer.produce(cdm_categ_msg)
                self._logger.info(f"{datetime.utcnow()}. Message cdm_categ_msg Sent")

            # CREATE TABLE cdm.user_product_counters (
            # 	id serial4 NOT NULL,
            # 	user_id int8 NOT NULL,
            # 	product_id int8 NOT NULL,
            # 	product_name varchar(100) NOT NULL,
            # 	order_cnt int4 NOT NULL,


            # CREATE TABLE cdm.user_category_counters (
            # 	id serial4 NOT NULL,
            # 	user_id int8 NOT NULL,
            # 	category_id int8 NOT NULL,
            # 	category_name varchar(100) NOT NULL,
            # 	order_cnt int4 NOT NULL,         

        self._logger.info(f"{datetime.utcnow()}: FINISH2")
