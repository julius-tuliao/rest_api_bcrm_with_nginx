from typing import Dict
from psycopg2 import pool
from app.config import AMEYO_DATABASE_HOST,AMEYO_DATABASE_USER,AMEYO_DATABASE_PASSWORD,AMEYO_PORT

class AMEYOConnectionPool:
    __instances: Dict[str, pool.SimpleConnectionPool] = {}

    def __init__(self, database_name):
        if database_name in AMEYOConnectionPool.__instances:
            # print('This database connection already exists!')
            self.__pool = AMEYOConnectionPool.__instances[database_name]
        else:
            AMEYOConnectionPool.__instances[database_name] = pool.SimpleConnectionPool(
                1,
                20,
                host=AMEYO_DATABASE_HOST,
                database=database_name,
                user=AMEYO_DATABASE_USER,
                password=AMEYO_DATABASE_PASSWORD,
                port=AMEYO_PORT
            )
            self.__pool = AMEYOConnectionPool.__instances[database_name]
            
    def __enter__(self):
        self.conn = self.__pool.getconn()
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__pool.putconn(self.conn)
        
    def getInstance(self):
        return self.__pool
    