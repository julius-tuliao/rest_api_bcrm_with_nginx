from typing import Dict
from psycopg2 import pool
from app.config import RDS_DATABASE_HOST, RDS_DATABASE_USER, RDS_DATABASE_PASSWORD, RDS_PORT, RDS_DATABASE_NAME


class AWSConnectionPool:
    __instances: Dict[str, pool.SimpleConnectionPool] = {}

    def __init__(self, database_name):
        if database_name in AWSConnectionPool.__instances:
            # print('This database connection already exists!')
            self.__pool = AWSConnectionPool.__instances[database_name]
        else:
            AWSConnectionPool.__instances[database_name] = pool.SimpleConnectionPool(
                1,
                1,
                host=RDS_DATABASE_HOST,
                database=database_name,
                user=RDS_DATABASE_USER,
                password=RDS_DATABASE_PASSWORD,
                port=RDS_PORT
            )
            self.__pool = AWSConnectionPool.__instances[database_name]
            
    def __enter__(self):
        self.conn = self.__pool.getconn()
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__pool.putconn(self.conn)
        
    def getInstance(self):
        return self.__pool
    


main_db = AWSConnectionPool(RDS_DATABASE_NAME)
