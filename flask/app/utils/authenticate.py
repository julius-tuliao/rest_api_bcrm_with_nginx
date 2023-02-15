from functools import wraps
from flask_restful import request
from app.databases.aws import main_db
from app.config import SECRET_KEY
import jwt
SELECT_USER_RETURN_ID = "SELECT * FROM api_users WHERE public_id = (%s) LIMIT 1"

def token_required(f):
    @wraps(f)
    def decorator(*args, **kwargs):

        token = None

        if 'x-access-tokens' in request.headers:
            token = request.headers['x-access-tokens']

        if not token:
            return {'message': 'a valid token is missing'}, 401
        connection = main_db.get_conn()
        try:
            data = jwt.decode(
                token, SECRET_KEY, algorithms=['HS256'])

            with connection.cursor() as cursor:
                cursor.execute(SELECT_USER_RETURN_ID, (data['public_id'],))
                current_user = cursor.fetchone()

                # Add the user's role to the decoded token data
                data[5] = current_user[5]

            main_db.put_conn(connection)

        except Exception as ex:
            main_db.put_conn(connection)

            print(ex)
            return {'message': 'token is invalid'}, 401

        return f(current_user, *args, **kwargs)

    return decorator