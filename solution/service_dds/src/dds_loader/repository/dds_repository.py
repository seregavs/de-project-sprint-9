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

    def getCategory_id(self: any, category_name: str) -> str:
        return uuid.uuid5(namespace=self._order_ns_uuid, name=category_name)  

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
                       order_dt: str,
                       cost: float,
                       payment: float,
                       status: str) -> None:
        
        h_order_pk = self._uuid(order_id)
        hk_order_status_hashdiff = self._uuid([order_id, status])
        hk_order_cost_hashdiff = self._uuid([order_id, cost, payment])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_order(
                        h_order_pk,
                        order_id,
                        order_dt,   
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(order_id)s,
                        %(order_dt)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_order_pk': h_order_pk,
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_order_status(
                        h_order_pk,
                        status,
                        load_dt,
                        load_src,
                        hk_order_status_hashdiff
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(status)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_order_status_hashdiff)s
                    )
                    ON CONFLICT (hk_order_status_hashdiff) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_order_pk': h_order_pk,
                        'status': status,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system,
                        'hk_order_status_hashdiff': hk_order_status_hashdiff
                    }
                )
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_order_cost(
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src,
                        hk_order_cost_hashdiff
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(cost)s,
                        %(payment)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_order_cost_hashdiff)s
                    )
                    ON CONFLICT (hk_order_cost_hashdiff) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'h_order_pk': h_order_pk,
                        'cost': cost,
                        'payment': payment,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system,
                        'hk_order_cost_hashdiff': hk_order_cost_hashdiff
                    }
                )

# Product-Category DML
    def product_category_insert(self,
                       product_id: str,
                       category_name: str) -> None:
        
        h_product_pk = self._uuid(product_id)
        h_category_pk = self._uuid(category_name)
        hk_product_category_pk = self._uuid([product_id, category_name])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_product_category(
                        hk_product_category_pk,
                        h_category_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_product_category_pk)s,
                        %(h_category_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_category_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'hk_product_category_pk': hk_product_category_pk,
                        'h_category_pk': h_category_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )

# Product-Restaurant DML
    def product_restaurant_insert(self,
                       product_id: str,
                       restaurant_id: str) -> None:
        
        h_product_pk = self._uuid(product_id)
        h_restaurant_pk = self._uuid(restaurant_id)
        hk_product_restaurant_pk = self._uuid([product_id, restaurant_id])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_product_restaurant(
                        hk_product_restaurant_pk,
                        h_restaurant_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_product_restaurant_pk)s,
                        %(h_restaurant_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'hk_product_restaurant_pk': hk_product_restaurant_pk,
                        'h_restaurant_pk': h_restaurant_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )

# Order-Product DML
    def order_product_insert(self,
                    order_id: str,
                    product_id: str) -> None:
        
        h_order_pk = self._uuid(order_id)
        h_product_pk = self._uuid(product_id)
        hk_order_product_pk = self._uuid([order_id, product_id])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_order_product(
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_order_product_pk)s,
                        %(h_order_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_product_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'hk_order_product_pk': hk_order_product_pk,
                        'h_order_pk': h_order_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )

# Order-User DML
    def order_user_insert(self,
                    order_id: str,
                    user_id: str) -> None:
        
        h_order_pk = self._uuid(order_id)
        h_user_pk = self._uuid(user_id)
        hk_order_user_pk = self._uuid([order_id, user_id])

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_order_user(
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_order_user_pk)s,
                        %(h_order_pk)s,
                        %(h_user_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_user_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'hk_order_user_pk': hk_order_user_pk,
                        'h_order_pk': h_order_pk,
                        'h_user_pk': h_user_pk,
                        'load_dt': datetime.utcnow(),
                        'load_src': self.source_system
                    }
                )
