import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
# from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self._order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')
        self.source_system = "orders-system-kafka"

    def _uuid(self, obj: any) -> uuid.UUID:
        return uuid.uuid5(namespace=self._order_ns_uuid, name=str(obj))    

# Category DML
    def category_insert(self,
                       category_name: str) -> None:
        
        h_category_pk = self._uuid(category_name)

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_category(
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_category_pk)s,
                        %(category_name)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_category_pk) DO UPDATE
                    SET
                        category_name = EXCLUDED.category_name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_category_pk': h_category_pk,
                        'category_name': category_name,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )
    
# Product DML
    def product_insert(self,
                       product_id: str,
                       name: str) -> None:
        
        h_product_pk = self._uuid(product_id)
        hk_product_names_hashdiff = self._uuid([product_id, name])
# https://practicum.yandex.ru/learn/data-engineer/courses/c801b240-75b5-4491-8bba-14c2e3ff4b9c/sprints/129468/topics/e1834338-a62f-4a0e-bb1e-c36a43773e6f/lessons/c20cd7cc-936d-48a7-8259-60cfc0cf419c/
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_product(
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(product_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_product_pk) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_product_pk': h_product_pk,
                        'product_id': product_id,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_product_names(
                        h_product_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_product_names_hashdiff
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_product_names_hashdiff)s
                    )
                    ON CONFLICT (hk_product_names_hashdiff) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_product_pk': h_product_pk,
                        'name': name,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system,
                        'hk_product_names_hashdiff': hk_product_names_hashdiff
                    }
                )                

# Restaurant DML
    def restaurant_insert(self,
                       restaurant_id: str,
                       name: str) -> None:
        
        h_restaurant_pk = self._uuid(restaurant_id)
        hk_restaurant_names_hashdiff = self._uuid([restaurant_id, name])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_restaurant(
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(restaurant_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'restaurant_id': restaurant_id,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_restaurant_names(
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_restaurant_names_hashdiff)s
                    )
                    ON CONFLICT (hk_restaurant_names_hashdiff) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'name': name,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system,
                        'hk_restaurant_names_hashdiff': hk_restaurant_names_hashdiff
                    }
                )  

# User DML
    def user_insert(self,
                       user_id: str,
                       userlogin: str,
                       username: str) -> None:
        
        h_user_pk = self._uuid(user_id)
        hk_user_names_hashdiff = self._uuid([user_id, userlogin, username])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_user(
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_user_pk)s,
                        %(userid)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_user_pk) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_user_pk': h_user_pk,
                        'user_id': user_id,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_user_names(
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src,
                        hk_user_names_hashdiff
                    )
                    VALUES(
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_user_names_hashdiff)s
                    )
                    ON CONFLICT (hk_user_names_hashdiff) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_user_pk': h_user_pk,
                        'username': username,
                        'userlogin': userlogin,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system,
                        'hk_user_names_hashdiff': hk_user_names_hashdiff
                    }
                ) 

# Order DML
    def order_insert(self,
                       order_id: str,
                       name: str) -> None:
        
        h_restaurant_pk = self._uuid(order_id)
        hk_restaurant_names_hashdiff = self._uuid([restaurant_id, name])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_restaurant(
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(restaurant_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'restaurant_id': restaurant_id,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_restaurant_names(
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_restaurant_names_hashdiff)s
                    )
                    ON CONFLICT (hk_restaurant_names_hashdiff) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'name': name,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system,
                        'hk_restaurant_names_hashdiff': hk_restaurant_names_hashdiff
                    }
                )