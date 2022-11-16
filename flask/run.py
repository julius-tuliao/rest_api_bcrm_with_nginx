import os
import psycopg2
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import uuid
import jwt
from dotenv import load_dotenv
from flask import Flask,request, jsonify, make_response


load_dotenv()

INSERT_STATUS_RETURN_ID = "INSERT INTO leads_result (leads_result_lead,leads_result_address,leads_result_contact,leads_result_source,leads_result_sdate,leads_result_edate,leads_result_amount,leads_result_ornumber,leads_result_comment,leads_result_users,leads_result_status_id,leads_result_substatus_id,leads_result_barcode_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING leads_result_id;"
REGISTER_USER_RETURN_ID = "INSERT INTO api_users(public_id,name,password) VALUES (%s,%s,%s) RETURNING ID;"
SELECT_USER_RETURN_ID= "SELECT * FROM api_users WHERE public_id = (%s) LIMIT 1"
SELECT_USER_BY_USER_AND_PASS = "SELECT * FROM api_users WHERE name = (%s)  LIMIT 1"
SELECT_CH_RETURN_ID = "SELECT leads_id,leads_client_id FROM leads WHERE leads_chcode = (%s) LIMIT 1"
SELECT_STATUS_RETURN_ID = "SELECT leads_status_id FROM leads_status WHERE leads_status_client_id = (%s) AND leads_status_name = (%s) AND leads_status_deleted = 0  LIMIT 1"
SELECT_SUBSTATUS_RETURN_ID = "SELECT leads_substatus_id FROM leads_substatus WHERE leads_substatus_status_id = (%s) AND leads_substatus_name = (%s) AND leads_substatus_deleted = 0 LIMIT 1"
SELECT_AGENT_RETURN_ID = "SELECT users_id FROM users WHERE users_username = (%s) ORDER BY users_id DESC LIMIT 1  "

app = Flask(__name__)

app.config['SECRET_KEY']=os.getenv("SECRET_KEY")

db_host = os.getenv("DATABASE_HOST")
db_name = os.getenv("DATABASE_NAME")
db_user = os.getenv("DATABASE_USER")
db_password = os.getenv("DATABASE_PASSWORD")



def connect_to_database():
    
    connection = None
    
    try:
        connection=psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password)
        return connection 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return False

def token_required(f):
   @wraps(f)
   def decorator(*args, **kwargs):

    connection = connect_to_database()

    token = None

    if 'x-access-tokens' in request.headers:
        token = request.headers['x-access-tokens']

    if not token:
        return jsonify({'message': 'a valid token is missing'})

    try:
         data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
         with connection:

            with connection.cursor() as cursor:
                cursor.execute(SELECT_USER_RETURN_ID,(data['public_id'],))
                current_user = cursor.fetchone()

    except Exception as ex:
         print(ex)
         return jsonify({'message': 'token is invalid'})

    return f(current_user, *args, **kwargs)

   return decorator


@app.route('/login', methods=['GET', 'POST'])  
def login_user(): 
 
  auth = request.authorization   

  connection = connect_to_database()

  if not auth or not auth.username or not auth.password:  
     return make_response('could not verify', 401, {'WWW.Authentication': 'Basic realm: "login required"'})    

  
  with connection:

            with connection.cursor() as cursor:
                cursor.execute(SELECT_USER_BY_USER_AND_PASS,(auth.username,))
                user = cursor.fetchone()
               
                
    
  if check_password_hash(user[3], auth.password):  
     
     token = jwt.encode({'public_id': user[1], 'exp' : datetime.datetime.utcnow() + datetime.timedelta(minutes=300)}, app.config['SECRET_KEY'])  
     
     return jsonify({'token' : token}) 

  return make_response('could not verify',  401, {'WWW.Authentication': 'Basic realm: "login required"'})


@app.route('/register', methods=['GET', 'POST'])
@token_required
def signup_user(current_user):  
 data = request.get_json()  

 hashed_password = generate_password_hash(data['password'], method='sha256')
 name = data["name"]
 
 connection = connect_to_database()

 with connection:

        with connection.cursor() as cursor:
            cursor.execute(REGISTER_USER_RETURN_ID,(str(uuid.uuid4()), name, hashed_password,))
            user_id = cursor.fetchone()[0]

 return {"id": user_id, "message": f"status {name} created."},201


@app.route('/api/status', methods=['POST', 'GET'])
@token_required
def create_status(current_user):
    data = request.get_json()

    connection = connect_to_database()

    try:
        address = data["address"]
        ch_code = data["ch_code"]
        contact = data["contact"]
        source = data["source"]
        start_date = data["start_date"]
        end_date = data["end_date"]
        amount = data["amount"]
        or_number = data["or_number"]
        comment = data["comment"]
        agent = data["agent"]
        disposition_class = data["disposition_class"]
        disposition_code = data["disposition_code"]
        barcode_date_with_tz = datetime.datetime.utcnow()
        barcode_date = barcode_date_with_tz.replace(tzinfo=None)
        with connection:

            with connection.cursor() as cursor:
                # Find Client ID
                cursor.execute(SELECT_CH_RETURN_ID,(ch_code,))
                row = cursor.fetchone()
                if row != None:
                    
                    leads_id = row[0]
                    leads_client_id = row[1]    
                                
                else:   
                    return {"error": "Client Does Not Exist"},404
                    

                # Find Agent ID
                cursor.execute(SELECT_AGENT_RETURN_ID,(agent,))
                row = cursor.fetchone()
                if row != None:
                    agent_id = row[0]                              
                else:
                    return {"error": "Agent Does Not Exist"},404
                

                # Find Status
                cursor.execute(SELECT_STATUS_RETURN_ID,(leads_client_id,disposition_class,))          
                row = cursor.fetchone()
                if row != None:   
                    leads_status_id = row[0]
                else:
                    return {"error": "Status Does Not Exist"},404

               
                cursor.execute(SELECT_SUBSTATUS_RETURN_ID,(leads_status_id,disposition_code,))
                row = cursor.fetchone()
                if row != None:
                    leads_substatus_id = row[0]
                else:
                    return {"error": "Sub Status Does Not Exist"},404

                
                cursor.execute(INSERT_STATUS_RETURN_ID,(leads_id,address,contact,source,start_date,end_date,amount,or_number,comment,agent_id,leads_status_id,leads_substatus_id,barcode_date))           
                row = cursor.fetchone()
                if row != None:
                    result_id = row[0]
                
                else:
                    return {"error": "Error Inserting Data"},404

                


        return {"id": result_id, "message": f"status {ch_code} created."},201

    except Exception as ex:

        return {"error": f"{ex}"},400

if __name__ == ('__main__'):

    app.run(host='0.0.0.0')

