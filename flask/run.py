import os
import psycopg2
import psycopg2.extras
import datetime
import math
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import uuid
import jwt
from dotenv import load_dotenv
from flask import Flask,request, jsonify, make_response
from psycopg2 import sql

load_dotenv()

INSERT_STATUS_RETURN_ID = "INSERT INTO leads_result (leads_result_lead,leads_result_address,leads_result_contact,leads_result_source,leads_result_sdate,leads_result_edate,leads_result_amount,leads_result_ornumber,leads_result_comment,leads_result_users,leads_result_status_id,leads_result_substatus_id,leads_result_barcode_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING leads_result_id;"
REGISTER_USER_RETURN_ID = "INSERT INTO api_users(public_id,name,password) VALUES (%s,%s,%s) RETURNING ID;"
SELECT_USER_RETURN_ID= "SELECT * FROM api_users WHERE public_id = (%s) LIMIT 1"
SELECT_USER_BY_USER_AND_PASS = "SELECT * FROM api_users WHERE name = (%s)  LIMIT 1"
SELECT_CH_RETURN_ID = "SELECT leads_id,leads_client_id FROM leads WHERE leads_chcode = (%s) LIMIT 1"
SELECT_STATUS_RETURN_ID = "SELECT leads_status_id FROM leads_status WHERE leads_status_client_id = (%s) AND leads_status_name = (%s) AND leads_status_deleted = 0  LIMIT 1"
SELECT_SUBSTATUS_RETURN_ID = "SELECT leads_substatus_id FROM leads_substatus WHERE leads_substatus_status_id = (%s) AND leads_substatus_name = (%s) AND leads_substatus_deleted = 0 LIMIT 1"
SELECT_AGENT_RETURN_ID = "SELECT users_id FROM users WHERE users_username = (%s) ORDER BY users_id DESC LIMIT 1  "
SELECT_FIELD_TASK_STAGE_NAME_RETURN_ID = "SELECT field_task_stages_id FROM field_task_stages WHERE field_task_stages_name = (%s) ORDER BY field_task_stages_id DESC LIMIT 1"
SELECT_FIELD_OFFICER_RETURN_ID = "SELECT field_officers_id FROM field_officers WHERE field_officers_id = (%s) ORDER BY field_officers_id DESC LIMIT 1"
SELECT_FIELD_TASKS_COUNT = sql.SQL("""
                SELECT COUNT(*) AS "count" FROM field_tasks
                INNER JOIN field_task_stages ON field_task_stages_id = field_tasks_stage
                INNER JOIN field_task_types ON field_task_types_id = field_tasks_type_id
                INNER JOIN leads ON leads_id = field_tasks_leads_id
                INNER JOIN client ON client_id = leads_client_id
                INNER JOIN users agent ON agent.users_id = leads_users_id
                INNER JOIN users requested_by ON requested_by.users_username = field_tasks_requested_by {where}""")
SELECT_FIELD_TASKS = sql.SQL("""SELECT 
                field_tasks_id,
                field_task_stages_name,
                field_task_types_name,
                field_tasks_reference_code,
                field_tasks_address_type,
                field_tasks_address,
                field_tasks_area,
                leads_chcode,
                leads_chname,
                client_name,
                agent.users_username AS agent_code,
                agent.users_name AS agent_name,
                field_tasks_source,
                field_tasks_created_at,
                field_tasks_remarks,
                field_tasks_requested_by AS requested_by_code,
                requested_by.users_name AS requested_by_name,
                field_officers_id,
                field_officers_name,
                field_officers_email,
                field_officers_lark_user_id
                FROM field_tasks
                INNER JOIN field_task_stages ON field_task_stages_id = field_tasks_stage
                INNER JOIN field_task_types ON field_task_types_id = field_tasks_type_id
                INNER JOIN leads ON leads_id = field_tasks_leads_id
                INNER JOIN client ON client_id = leads_client_id
                INNER JOIN users agent ON agent.users_id = leads_users_id
                INNER JOIN users requested_by ON requested_by.users_username = field_tasks_requested_by
                LEFT JOIN field_officers ON field_officers_id = field_tasks_field_officer_id
                {where}
                ORDER BY {sort} {dir} LIMIT {limit} OFFSET {offset}""")
SELECT_FIELD_TASK = sql.SQL("""SELECT 
                field_tasks_id,
                field_task_stages_name,
                field_task_types_name,
                field_tasks_reference_code,
                field_tasks_address_type,
                field_tasks_address,
                field_tasks_area,
                leads_chcode,
                leads_chname,
                client_name,
                agent.users_username AS agent_code,
                agent.users_name AS agent_name,
                field_tasks_source,
                field_tasks_created_at,
                field_tasks_remarks,
                field_tasks_requested_by AS requested_by_code,
                requested_by.users_name AS requested_by_name,
                field_officers_id,
                field_officers_name,
                field_officers_email,
                field_officers_lark_user_id
                FROM field_tasks
                INNER JOIN field_task_stages ON field_task_stages_id = field_tasks_stage
                INNER JOIN field_task_types ON field_task_types_id = field_tasks_type_id
                INNER JOIN leads ON leads_id = field_tasks_leads_id
                INNER JOIN client ON client_id = leads_client_id
                INNER JOIN users agent ON agent.users_id = leads_users_id
                INNER JOIN users requested_by ON requested_by.users_username = field_tasks_requested_by
                LEFT JOIN field_officers ON field_officers_id = field_tasks_field_officer_id
                WHERE field_tasks_id = {field_tasks_id}""")
INSERT_FIELD_RESULT_RETURN_ID = "INSERT INTO field_results(field_results_field_tasks_id, field_results_status_id, field_results_sub_status_id, field_results_field_officer_id, field_results_submitted_at, field_results_remarks, field_results_email, field_results_informant, field_results_informant_name, field_results_informant_relationship, field_results_amount, field_results_amount_date, field_results_location_latitude, field_results_location_longitude, field_results_reported_to_users_id, field_results_officer_in_tag_users_id, field_results_client_availability) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING field_results_id"
SELECT_FIELD_STATUS_OPTIONS = "SELECT field_statuses_id, field_statuses_name FROM field_statuses WHERE field_statuses_deleted_at IS NULL"
SELECT_FIELD_SUB_STATUS_OPTIONS = "SELECT field_sub_statuses_id, field_sub_statuses_name FROM field_sub_statuses INNER JOIN field_statuses ON field_statuses_id = field_sub_statuses_field_status_id WHERE field_sub_statuses_deleted_at IS NULL AND field_statuses_deleted_at IS NULL AND field_sub_statuses_field_status_id = (%s)"
SELECT_BCRM_USERS_OPTIONS = "SELECT users_id,users_username,users_name,user_global_id FROM users WHERE users_deleted = 0"

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
        return {'message': 'a valid token is missing'},498

    try:
         data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
         with connection:

            with connection.cursor() as cursor:
                cursor.execute(SELECT_USER_RETURN_ID,(data['public_id'],))
                current_user = cursor.fetchone()

    except Exception as ex:
         print(ex)
         return {'message': 'token is invalid'},498

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
        disposition_class = data["disposition_class"].split('-')[0].strip()
        disposition_code = data["disposition_code"].split('-')[0].strip()
        barcode_date_with_tz = datetime.datetime.utcnow()
        barcode_date = barcode_date_with_tz.replace(tzinfo=None)

        if(len(start_date) < 3):
            start_date = None

        if(len(end_date) < 3):
            end_date = None

        if(len(amount) < 2):
            amount = 0

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

@app.route('/api/field_tasks', methods=['GET'])
@token_required
def field_tasks(current_user):
    connection = connect_to_database()

    directions = ["ASC","DESC"]
    sortables = ['field_tasks_id','field_tasks_created_at']

    limit = request.args.get('limit', 25, type=int)
    offset = request.args.get('offset', 0, type=int)
    sort = request.args.get('sort',type=str)
    dir = request.args.get('dir',type=str)

    try:

        with connection:

            with connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cursor:
                # Filters
                where = sql.SQL("WHERE true")

                search_reference_code = request.args.get('reference_code',type=str)
                if search_reference_code:
                    where += sql.SQL(" AND field_tasks_reference_code LIKE {}").format(sql.Literal("%"+search_reference_code+"%"))
                
                field_officers_id = request.args.get('field_officer_id',type=int)
                if field_officers_id:
                    # Find Field Officer
                    cursor.execute(SELECT_FIELD_OFFICER_RETURN_ID,(field_officers_id,))
                    row = cursor.fetchone()
                    if row != None:   
                        where += sql.SQL(" AND field_tasks_field_officer_id = {}").format(sql.Literal(row['field_officers_id']))
                    else:
                        return {"error": "Field Officer Does Not Exist"},404

                stage = request.args.get('stage_name',type=str)
                if stage:
                    # Find Field Stage
                    cursor.execute(SELECT_FIELD_TASK_STAGE_NAME_RETURN_ID,(stage,))
                    row = cursor.fetchone()
                    if row != None:   
                        where += sql.SQL(" AND field_tasks_stage = {}").format(sql.Literal(row['field_task_stages_name']))
                    else:
                        return {"error": "Field Tasks Stage Does Not Exist"},404

                # Defaults
                sort = sort if sort in sortables else sortables[0]
                dir = dir if dir in directions else directions[0]

                # For Pagination
                cursor.execute(SELECT_FIELD_TASKS_COUNT.format(
                    where = where,
                ))
                row = cursor.fetchone()
                total_rows = row['count']
                pages = math.ceil(total_rows/limit) # this is the number of pages

                # Data                
                cursor.execute(SELECT_FIELD_TASKS.format(
                    where = where,
                    sort = sql.Identifier(sort),
                    dir = sql.SQL(dir),
                    limit = sql.Literal(limit),
                    offset = sql.Literal(offset)
                ))
                data = cursor.fetchall()

        # Pagination options
        page = math.ceil((offset - 1) / limit) + 1
        last = limit * (pages - 1) if pages > 0 else 0
        next = offset + limit if (offset+limit) <= total_rows else offset
        prev = offset - limit if (offset-limit) > 0 else 0

        return {
                "offset": offset,
                "limit": limit,
                "total": total_rows,
                "page":{
                    "offset":{
                        "first":0,
                        "prev":prev,
                        "next":next,
                        "last":last
                    },
                    "current": page,
                    "total": pages
                },
                "sort":sort,
                "dir":dir,
                "data":data
            },200

    except Exception as ex:

        return {"error": f"{ex}"},400

@app.route('/api/field_tasks/read', methods=['GET'])
@token_required
def field_tasks_read(current_user):
    connection = connect_to_database()
    
    field_tasks_id = request.args.get('field_tasks_id', type=int)
    
    try:

        with connection:

            with connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cursor:
                
                # Data
                cursor.execute(SELECT_FIELD_TASK.format(
                    field_tasks_id = sql.Literal(field_tasks_id)
                ))
                row = cursor.fetchone()
                if row == None:   
                    return {"error": "Field Task Does Not Exist"},404

        return {"data":row},200

    except Exception as ex:

        return {"error": f"{ex}"},400

@app.route('/api/field_results', methods=['POST'])
@token_required
def field_results_create(current_user):
    connection = connect_to_database()

    data = request.get_json()
    
    try:
        field_results_field_tasks_id = data['field_results_field_tasks_id']
        field_results_status_id = data['field_results_status_id']
        field_results_sub_status_id = data['field_results_sub_status_id']
        field_results_field_officer_id = data['field_results_field_officer_id']
        field_results_submitted_at = data['field_results_submitted_at']
        field_results_remarks = data['field_results_remarks']
        field_results_email = data['field_results_email']
        field_results_informant = data['field_results_informant']
        field_results_informant_name = data['field_results_informant_name']
        field_results_informant_relationship = data['field_results_informant_relationship']
        field_results_amount = data['field_results_amount']
        field_results_amount_date = data['field_results_amount_date']
        # field_results_location_barangay_id = data['field_results_location_barangay_id']
        # field_results_location_city_municipality_id = data['field_results_location_city_municipality_id']
        # field_results_location_subdivision_id = data['field_results_location_subdivision_id']
        field_results_location_latitude = data['field_results_location_latitude']
        field_results_location_longitude = data['field_results_location_longitude']
        field_results_reported_to_users_id = data['field_results_reported_to_users_id']
        field_results_officer_in_tag_users_id = data['field_results_officer_in_tag_users_id']
        field_results_client_availability = data['field_results_client_availability']

        with connection:

            with connection.cursor() as cursor:
                
                # Data
                cursor.execute(INSERT_FIELD_RESULT_RETURN_ID,(
                    field_results_field_tasks_id,
                    field_results_status_id,
                    field_results_sub_status_id,
                    field_results_field_officer_id,
                    field_results_submitted_at,
                    field_results_remarks,
                    field_results_email,
                    field_results_informant,
                    field_results_informant_name,
                    field_results_informant_relationship,
                    field_results_amount,
                    field_results_amount_date,
                    field_results_location_latitude,
                    field_results_location_longitude,
                    field_results_reported_to_users_id,
                    field_results_officer_in_tag_users_id,
                    field_results_client_availability
                ))           
                row = cursor.fetchone()
                if row != None:
                    result_id = row[0]
                else:
                    return {"error": "Error Inserting Field Result"},404

        return {"data":result_id},200

    except Exception as ex:

        return {"error": f"{ex}"},400

@app.route('/api/field_statuses/options', methods=['GET'])
@token_required
def field_status_options(current_user):
    connection = connect_to_database()
    try:

        with connection:

            with connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cursor:
                
                # Data
                cursor.execute(SELECT_FIELD_STATUS_OPTIONS)
                data = cursor.fetchall()

        return {"data":data},200

    except Exception as ex:

        return {"error": f"{ex}"},400

@app.route('/api/field_sub_statuses/options', methods=['GET'])
@token_required
def field_sub_status_options(current_user):
    connection = connect_to_database()

    field_status_id = request.args.get('field_status_id',0, type=int)
    try:

        with connection:

            with connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cursor:
                
                # Data
                cursor.execute(SELECT_FIELD_SUB_STATUS_OPTIONS,(field_status_id,))
                data = cursor.fetchall()

        return {"data":data},200

    except Exception as ex:

        return {"error": f"{ex}"},400

@app.route('/api/users/options', methods=['GET'])
@token_required
def bcrm_users_options(current_user):
    connection = connect_to_database()
    
    field_status_id = request.args.get('field_status_id',0, type=int)

    try:

        with connection:

            with connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cursor:
                
                # Data
                cursor.execute(SELECT_BCRM_USERS_OPTIONS)
                data = cursor.fetchall()

        return {"data":data},200

    except Exception as ex:

        return {"error": f"{ex}"},400

if __name__ == ('__main__'):

    app.run(host='0.0.0.0')

