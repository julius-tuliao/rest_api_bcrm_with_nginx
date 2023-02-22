from typing import Dict
import psycopg2
from psycopg2 import pool
from app.config import AMEYO_DATABASE_HOST,AMEYO_DATABASE_USER,AMEYO_DATABASE_PASSWORD,AMEYO_PORT

class AMEYOConnection:
    __instances: Dict[str, pool.SimpleConnectionPool] = {}

    @staticmethod
    def getInstance(database_name):
        if database_name not in AMEYOConnection.__instances:
            AMEYOConnection.__instances[database_name] = AMEYOConnection(
                database_name)
        return AMEYOConnection.__instances[database_name]

    def __init__(self, database_name):
        if database_name in AMEYOConnection.__instances:
            # print('This database connection already exists!')
            self.__connection_pool = self.__instances[database_name]
        else:
            self.__instances[database_name] = psycopg2.pool.SimpleConnectionPool(
                1,
                20,
                host=AMEYO_DATABASE_HOST,
                database=database_name,
                user=AMEYO_DATABASE_USER,
                password=AMEYO_DATABASE_PASSWORD,
                port=AMEYO_PORT
            )

            self.__connection_pool = self.__instances[database_name]

    def get_conn(self) -> pool.SimpleConnectionPool.getconn:
        return self.__connection_pool.getconn()

    def put_conn(self,connection):
        return self.__connection_pool.putconn(connection)

    def execute_query(self, query, values=None):
        connection = self.__connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()
        result = cursor.fetchall()
        cursor.close()
        self.__connection_pool.putconn(connection)
        return result

