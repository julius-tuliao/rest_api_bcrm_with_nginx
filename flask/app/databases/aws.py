from typing import Dict
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from app.config import RDS_DATABASE_HOST, RDS_DATABASE_USER, RDS_DATABASE_PASSWORD, RDS_PORT


class AWSConnection:
    __instances: Dict[str, pool.SimpleConnectionPool] = {}

    @staticmethod
    def getInstance(database_name):
        if database_name not in AWSConnection.__instances:
            AWSConnection.__instances[database_name] = AWSConnection(
                database_name)
        return AWSConnection.__instances[database_name]

    def __init__(self, database_name):
        if database_name in AWSConnection.__instances:
            # print('This database connection already exists!')
            self.__connection_pool = self.__instances[database_name]
        else:
            self.__instances[database_name] = psycopg2.pool.SimpleConnectionPool(
                1,
                5,
                host=RDS_DATABASE_HOST,
                database=database_name,
                user=RDS_DATABASE_USER,
                password=RDS_DATABASE_PASSWORD,
                port=RDS_PORT
            )

            self.__connection_pool = self.__instances[database_name]

    def get_conn(self) -> pool.SimpleConnectionPool.getconn:
        conn = self.__connection_pool.getconn()
        return conn

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



main_db = AWSConnection('spm_db')
