import os
import psycopg2
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import uuid
import jwt
import dotenv
from flask import Flask,request, jsonify, make_response
from etl_upsert import ETL
from psycopg2.pool import ThreadedConnectionPool


#  Role ID Level
#  admin = 1
#  accounts_etl = 2
#  field = 3
#  agent_status = 4
#  itd = 5

dotenv.load_dotenv('dev.env')

INSERT_STATUS_RETURN_ID = "INSERT INTO leads_result (leads_result_lead,leads_result_address,leads_result_contact,leads_result_source,leads_result_sdate,leads_result_edate,leads_result_amount,leads_result_ornumber,leads_result_comment,leads_result_users,leads_result_status_id,leads_result_substatus_id,leads_result_barcode_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING leads_result_id;"
REGISTER_USER_RETURN_ID = "INSERT INTO api_users(public_id,name,password,role_id) VALUES (%s,%s,%s,%s) RETURNING ID;"
SELECT_USER_RETURN_ID= "SELECT * FROM api_users WHERE public_id = (%s) LIMIT 1"
SELECT_USER_BY_USER_AND_PASS = "SELECT * FROM api_users WHERE name = (%s)  LIMIT 1"
SELECT_CH_RETURN_ID = "SELECT leads_id,leads_client_id FROM leads WHERE leads_chcode = (%s) LIMIT 1"
SELECT_STATUS_RETURN_ID = "SELECT leads_status_id FROM leads_status WHERE leads_status_client_id = (%s) AND leads_status_name = (%s) AND leads_status_deleted = 0  LIMIT 1"
SELECT_SUBSTATUS_RETURN_ID = "SELECT leads_substatus_id FROM leads_substatus WHERE leads_substatus_status_id = (%s) AND leads_substatus_name = (%s) AND leads_substatus_deleted = 0 LIMIT 1"
SELECT_AGENT_RETURN_ID = "SELECT users_id FROM users WHERE users_username = (%s) ORDER BY users_id DESC LIMIT 1  "
INSERT_FIELD_STATUS_RETURN_ID  = "INSERT INTO nestform (chcode,reference,bank,status,json_data,area,field_name,date_created) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"
SELECT_RDS_DATABASE_RETURN_RDS = "SELECT rds_db_name FROM campaigns WHERE campaign_id = (%s) LIMIT 1"
app = Flask(__name__)


app.config['SECRET_KEY']=os.getenv("SECRET_KEY")

main_db_host = os.getenv("RDS_DATABASE_HOST")
main_db_name = os.getenv("RDS_DATABASE_NAME")
main_db_user = os.getenv("RDS_DATABASE_USER")
main_db_password = os.getenv("RDS_DATABASE_PASSWORD")

 
# Create a connection pool
pool = ThreadedConnectionPool(
    minconn=1,  # minimum number of connections in the pool
    maxconn=20,  # maximum number of connections in the pool
    host=main_db_host,
    database=main_db_name,
    user=main_db_user,
    password=main_db_password
)


def connect_to_main_database():
    global pool
    # Check if the connection pool has been closed
    if pool.closed:

        print("test")
        # Recreate the connection pool
        pool = ThreadedConnectionPool(
            minconn=1,  # minimum number of connections in the pool
            maxconn=20,  # maximum number of connections in the pool
            host=main_db_host,
            database=main_db_name,
            user=main_db_user,
            password=main_db_password
       )

    # Get a connection from the pool
    connection = pool.getconn()
        
    return connection

def connect_to_bank_database(db_name):
    global pool
    # Check if the connection pool has been closed
    if pool.closed:

        print("test")
        # Recreate the connection pool
        pool = ThreadedConnectionPool(
            minconn=1,  # minimum number of connections in the pool
            maxconn=20,  # maximum number of connections in the pool
            host=main_db_host,
            database=db_name,
            user=main_db_user,
            password=main_db_password
       )

    # Get a connection from the pool
    connection = pool.getconn()
        
    return connection

def token_required(f):
   @wraps(f)
   def decorator(*args, **kwargs):

    connection = connect_to_main_database()

    token = None

    if 'x-access-tokens' in request.headers:
        token = request.headers['x-access-tokens']

    if not token:
        return {'message': 'a valid token is missing'},498

    try:
         data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
         with connection:

            with connection.cursor() as cursor:
                cursor.execute(SELECT_USER_RETURN_ID,(data['public_id'],))
                current_user = cursor.fetchone()

                 # Add the user's role to the decoded token data
                data[5] = current_user[5]
            
         pool.putconn(connection)
  

    except Exception as ex:
         pool.putconn(connection)
  
         print(ex)
         return {'message': 'token is invalid'},498

    return f(current_user, *args, **kwargs)

   return decorator

def admin_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if admin user
        if data[5] != 1:
            return {"message": "admin privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def account_etl_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if etl user
        if data[5] != 2:
            return {"message": "Account ETL privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def field_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if field user
        if data[5] != 3:
            return {"message": "Field privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def agent_status_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if field user
        if data[5] != 4:
            return {"message": "Agent status privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def itd_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if itd user
        if data[5] != 5:
            return {"message": "ITD privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

@app.route('/login', methods=['GET', 'POST'])  
def login_user(): 
 
  auth = request.authorization   

  connection = connect_to_main_database()

  if not auth or not auth.username or not auth.password:  
     return make_response('could not verify', 401, {'WWW.Authentication': 'Basic realm: "login required"'})    

  
  with connection:

            with connection.cursor() as cursor:
                cursor.execute(SELECT_USER_BY_USER_AND_PASS,(auth.username,))
                user = cursor.fetchone()


  pool.putconn(connection)
               
                
    
  if check_password_hash(user[3], auth.password):  
     
     token = jwt.encode({'public_id': user[1], 'exp' : datetime.datetime.utcnow() + datetime.timedelta(minutes=300)}, app.config['SECRET_KEY'])  
     
     return jsonify({'token' : token}) 

  return make_response('could not verify',  401, {'WWW.Authentication': 'Basic realm: "login required"'})


@app.route('/register', methods=['GET', 'POST'])
@token_required
@admin_required
def signup_user(current_user):  
 data = request.get_json()  

 hashed_password = generate_password_hash(data['password'], method='sha256')
 name = data["name"]
 role = data["role"]
 
 connection = connect_to_main_database()

 with connection:

        with connection.cursor() as cursor:
            cursor.execute(REGISTER_USER_RETURN_ID,(str(uuid.uuid4()), name, hashed_password,role,))
            user_id = cursor.fetchone()[0]
  
 pool.putconn(connection)

 return {"id": user_id, "message": f"status {name} created."},201

 
@app.route('/get_accounts', methods=['POST'])
@token_required
@account_etl_required
def get_accounts():  
    bank_name = request.json['bank_name']

    # Create an ETL object
    etl = ETL()

    # Call the replicate() method
    etl.replicate(bank_name)

    return {"id": bank_name, "message": f"successfully sync"},200


@app.route('/api/status', methods=['POST', 'GET'])
@token_required
@agent_status_required
def create_status(current_user):
    data = request.get_json()

    main_db_connection = connect_to_main_database()

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
        disposition_class = data["disposition_class"].split('-')[0].strip()
        disposition_code = data["disposition_code"].split('-')[0].strip()
        barcode_date_with_tz = datetime.datetime.utcnow()
        barcode_date = barcode_date_with_tz.replace(tzinfo=None)
        campaign = data["campaign"]


        if(len(start_date) < 3):
            start_date = None

        if(len(end_date) < 3):
            end_date = None

        if(len(amount) < 2):
            amount = 0

        with main_db_connection:

            with main_db_connection.cursor() as cursor:

                 # Find Database
                cursor.execute(SELECT_RDS_DATABASE_RETURN_RDS,(campaign,))
                row = cursor.fetchone()
                if row != None:
                    
                    bank_db = row[0]
                                
                else:   
                    pool.putconn(main_db_connection)
  
                    return {"error": "Client Does Not Exist"},404
                    



        connection = connect_to_bank_database(bank_db)

        with connection:

            with connection.cursor() as cursor:
                # Find Client ID
                cursor.execute(SELECT_CH_RETURN_ID,(ch_code,))
                row = cursor.fetchone()
                if row != None:
                    
                    leads_id = row[0]
                    leads_client_id = row[1]    
                                
                else:   
                    pool.putconn(connection)
  
                    return {"error": "Client Does Not Exist"},404
                    

                # Find Agent ID
                cursor.execute(SELECT_AGENT_RETURN_ID,(agent,))
                row = cursor.fetchone()
                if row != None:
                    agent_id = row[0]                              
                else:
                    pool.putconn(connection)
  
                    return {"error": "Agent Does Not Exist"},404
                

                # Find Status
                cursor.execute(SELECT_STATUS_RETURN_ID,(leads_client_id,disposition_class,))          
                row = cursor.fetchone()
                if row != None:   
                    leads_status_id = row[0]
                else:
                    pool.putconn(connection)
  
                    return {"error": "Status Does Not Exist"},404

               
                cursor.execute(SELECT_SUBSTATUS_RETURN_ID,(leads_status_id,disposition_code,))
                row = cursor.fetchone()
                if row != None:
                    leads_substatus_id = row[0]
                else:
                    pool.putconn(connection)
  
                    return {"error": "Sub Status Does Not Exist"},404

                
                cursor.execute(INSERT_STATUS_RETURN_ID,(leads_id,address,contact,source,start_date,end_date,amount,or_number,comment,agent_id,leads_status_id,leads_substatus_id,barcode_date))           
                row = cursor.fetchone()
                if row != None:
                    result_id = row[0]
                
                else:
                    pool.putconn(connection)
  
                    return {"error": "Error Inserting Data"},404

                
        pool.putconn(connection)
  
        return {"id": result_id, "message": f"status {ch_code} created."},201

    except Exception as ex:
        pool.putconn(connection)
  
        return {"error": f"{ex}"},400


@app.route('/api/field_status', methods=['POST'])
@token_required
@field_required
def create_field_status(current_user):
    data = request.get_json()

    connection = connect_to_main_database()

    try:
        ch_code = data["chcode"]
        reference = data["reference"]
        bank = data["bank"]
        status = data["status"]
        area = data["area"]
        field_name = data["status"]
        json_data = data["json_data"]
        date_created = data["date_created"]

        with connection:

            with connection.cursor() as cursor:
                # Insert Field Status

                cursor.execute(INSERT_FIELD_STATUS_RETURN_ID,(ch_code,reference,bank,status,json_data,area,field_name,date_created))           
                row = cursor.fetchone()
                if row != None:

                    result_id = row[0]
                
                else:
                    pool.putconn(connection)
  
                    return {"error": "Error Inserting Data"},404

        pool.putconn(connection)
  
        return {"id": result_id, "message": f"field status {ch_code} created."},201
    
    except Exception as ex:
        pool.putconn(connection)
  
        return {"error": f"{ex}"},400


@app.route('/api/create_agent', methods=['POST', 'GET'])
@token_required
@itd_required
def create_agent(current_user):
    data = request.get_json()

    connection = connect_to_main_database()

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
        disposition_class = data["disposition_class"].split('-')[0].strip()
        disposition_code = data["disposition_code"].split('-')[0].strip()
        barcode_date_with_tz = datetime.datetime.utcnow()
        barcode_date = barcode_date_with_tz.replace(tzinfo=None)

        with connection:

            with connection.cursor() as cursor:
                    

                # Find Agent ID
                cursor.execute(SELECT_AGENT_RETURN_ID,(agent,))
                row = cursor.fetchone()
                if row != None:
                    agent_id = row[0]                              
                else:
                    return {"error": "Agent Does Not Exist"},404
                

                
                cursor.execute(INSERT_STATUS_RETURN_ID,(leads_id,address,contact,source,start_date,end_date,amount,or_number,comment,agent_id,leads_status_id,leads_substatus_id,barcode_date))           
                row = cursor.fetchone()
                if row != None:
                    result_id = row[0]
                
                else:
                    pool.putconn(connection)
  
                    return {"error": "Error Inserting Data"},404

                

        pool.putconn(connection)
  
        return {"id": result_id, "message": f"status {ch_code} created."},201

    except Exception as ex:

        pool.putconn(connection)

        return {"error": f"{ex}"},400


if __name__ == ('__main__'):

    app.run(host='0.0.0.0')

