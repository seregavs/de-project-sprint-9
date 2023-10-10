from pydantic import BaseModel
from typing import Any, Dict, List
from lib.pg import PgConnect

class User_Category_Counters(BaseModel):
    user_id: str
    category_id: str
    category_name: str
    order_cnt: int

class User_Product_Counters(BaseModel):
    user_id: str
    product_id: str
    product_name: str
    order_cnt: int

class CdmBuilder:
    def init(self, dict: Dict) -> None:
        self._dict = dict

    def user_category_counters(self) -> User_Category_Counters:
        return User_Category_Counters(
            user_id=self._dict["user_id"],
            category_id=self._dict["category_id"],
            category_name=self._dict["category_name"],
            order_cnt=self._dict["order_cnt"]
        )

    def user_product_counters(self) -> User_Product_Counters:
        return User_Product_Counters(
            user_id=self._dict["user_id"],
            product_id=self._dict["product_id"],
            product_name=self._dict["product_name"],
            order_cnt=self._dict["order_cnt"]
        )

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

# CDM User_Category DML
    def user_category_insert(self, msg: User_Category_Counters )-> None:
        
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO cdm.user_category_counters(
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                    )
                    VALUES(
                        %(user_id)s,
                        %(category_id)s,
                        %(category_name)s,
                        %(order_cnt)s
                    );
                """,
                    {
                        'user_id': msg.user_id,
                        'category_id': msg.category_id,
                        'category_name': msg.category_name,
                        'order_cnt': msg.order_cnt
                    }
                )

# CDM User_Product DML
    def user_product_insert(self, msg: User_Product_Counters ) -> None:
        
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO cdm.user_product_counters(
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                    )
                    VALUES(
                        %(user_id)s,
                        %(product_id)s,
                        %(product_name)s,
                        %(order_cnt)s
                    );
                """,
                    {
                        'user_id': msg.user_id,
                        'product_id': msg.product_id,
                        'product_name': msg.product_name,
                        'order_cnt': msg.order_cnt
                    }
                )