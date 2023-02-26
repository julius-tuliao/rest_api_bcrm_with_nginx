from app.utils import token_required, admin_required
import datetime
import uuid
from flask_restful import Resource, request
from app.config import SECRET_KEY
from app.databases.aws import main_db
from flask import make_response, jsonify
import jwt
from werkzeug.security import generate_password_hash, check_password_hash
REGISTER_USER_RETURN_ID = "INSERT INTO api_users(public_id,name,password,role_id) VALUES (%s,%s,%s,%s) RETURNING ID;"
SELECT_USER_BY_USER_AND_PASS = "SELECT * FROM api_users WHERE name = (%s)  LIMIT 1"


class UserResource(Resource):
    @token_required
    @admin_required
    def post(self, current_user):
        data = request.get_json()

        hashed_password = generate_password_hash(
            data['password'], method='sha256')
        name = data["name"]
        role = data["role"]

        main_db_connection = main_db.getconn()

        with main_db_connection:

            with main_db_connection.cursor() as cursor:

                cursor.execute(
                    'SELECT name FROM api_users WHERE name = %s', (name,))
                user_exist = cursor.fetchone()
                if user_exist:
                    cursor.close()
                    main_db.putconn(main_db_connection)
                    return {"message": f"Username {name} already exist."}, 400

                cursor.execute(REGISTER_USER_RETURN_ID, (str(
                    uuid.uuid4()), name, hashed_password, role,))
                user_id = cursor.fetchone()[0]

        main_db.putconn(main_db_connection)

        return {"id": user_id, "message": f"status {name} created."}, 201


class UserLoginResource(Resource):
    def post(self):

        try:
            auth = request.authorization

            if not auth or not auth.username or not auth.password:
                return make_response('could not verify', 401, {'WWW.Authentication': 'Basic realm: "login required"'})

            with main_db as conn:

                cursor = conn.cursor()
                cursor.execute(SELECT_USER_BY_USER_AND_PASS, (auth.username,))
                user = cursor.fetchone()


                if check_password_hash(user[3], auth.password):

                    token = jwt.encode({'public_id': user[1], 'exp': datetime.datetime.utcnow(
                    ) + datetime.timedelta(minutes=300)}, SECRET_KEY)

                    return jsonify({'token': token})

            return make_response('could not verify',  401, {'WWW.Authentication': 'Basic realm: "login required"'})
        except Exception as ex:
            print(ex)
            return make_response('could not verify',  401, {'WWW.Authentication': 'Basic realm: "login required"'})