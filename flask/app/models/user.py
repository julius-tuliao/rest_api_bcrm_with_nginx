from app.databases import main_db

class UserModel:
    
    def __init__(self):
        self.__db = main_db
        self.table_name = 'users'
    
    def get_by_id(self, user_id):
        query = f"SELECT * FROM {self.table_name} WHERE users_id = %s"
        result = self.__db.execute_query(query, (user_id,))
        return result if result else None
    
    def get(self):
        query = f"SELECT * FROM {self.table_name} WHERE users_id = %s"
        result = self.__db.execute_query(query,)
        return result if result else None

    def add_user(self, name, email, password):
        query = f"INSERT INTO {self.table_name} (name, email, password) VALUES (%s, %s, %s)"
        self.__db.execute_query(query, (name, email, password))

    def update_user(self, user_id, name, email, password):
        query = f"UPDATE {self.table_name} SET name = %s, email = %s, password = %s WHERE id = %s"
        self.__db.execute_query(query, (name, email, password, user_id))

    def delete_user(self, user_id):
        query = f"DELETE FROM {self.table_name} WHERE id = %s"
        self.__db.execute_query(query, (user_id,))
