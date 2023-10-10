from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_rep2 import DdsRepository, OrderDdsBuilder

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

            oddsb = OrderDdsBuilder(msg['payload'])

            with self._dds_repository as dr:
                dr.h_order_insert(oddsb.h_order())
                dr.h_restaurant_insert(oddsb.h_restaurant())
                dr.h_user_insert(oddsb.h_user())
                dr.s_order_cost_insert(oddsb.s_order_cost())
                dr.s_order_status_insert(oddsb.s_order_status())
                dr.s_restaurant_names_insert(oddsb.s_restaurant_names())
                dr.s_user_names_insert(oddsb.s_user_names())
                dr.l_order_user_insert(oddsb.l_order_user())

                for p in oddsb.h_product():
                    dr.h_product_insert(p)

                for c in oddsb.h_category():
                    dr.h_category_insert(c)
                
                for pn in oddsb.s_product_names():
                    dr.s_product_names_insert(pn)

                for pr in oddsb.l_product_restaurant():
                    dr.l_product_restaurant_insert(pr)

                for op in oddsb.l_order_product():
                    dr.l_order_product_insert(op)

                for pc in oddsb.l_product_category():
                    dr.l_product_category_insert(pc)

            for pl in oddsb.cdm_prd_msg():
                self._producer.produce(pl)
                self._logger.info(f"{datetime.utcnow()}. Message cdm_prd_msg sent") 

            for pl in oddsb.cdm_categ_msg():
                self._producer.produce(pl)
                self._logger.info(f"{datetime.utcnow()}. Message cdm_categ_msg sent")                      

        self._logger.info(f"{datetime.utcnow()}: FINISH2")