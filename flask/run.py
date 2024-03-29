from app import create_app

app = create_app()

if __name__ == ('__main__'):
    app.run(host='0.0.0.0')

# import os
# from app.config import SECRET_KEY
# from flask import Blueprint, Flask, jsonify
# from flask_restful import Api
# from app.utils.security import account_etl_required, admin_required, agent_status_required, field_required, itd_required
# import datetime
# from werkzeug.security import generate_password_hash, check_password_hash
# from functools import wraps
# import uuid
# import jwt
# from flask import Flask, request, jsonify, make_response
# from app.etl_upsert import ETL
# from app.databases.aws import main_db, AWSConnection



# #  Role ID Level
# #  admin = 1
# #  accounts_etl = 2
# #  field = 3
# #  agent_status = 4
# #  itd = 5

# INSERT_STATUS_RETURN_ID = "INSERT INTO leads_result (leads_result_lead,leads_result_address,leads_result_contact,leads_result_source,leads_result_sdate,leads_result_edate,leads_result_amount,leads_result_ornumber,leads_result_comment,leads_result_users,leads_result_status_id,leads_result_substatus_id,leads_result_barcode_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING leads_result_id;"
# REGISTER_USER_RETURN_ID = "INSERT INTO api_users(public_id,name,password,role_id) VALUES (%s,%s,%s,%s) RETURNING ID;"
# SELECT_USER_RETURN_ID = "SELECT * FROM api_users WHERE public_id = (%s) LIMIT 1"
# SELECT_USER_BY_USER_AND_PASS = "SELECT * FROM api_users WHERE name = (%s)  LIMIT 1"
# SELECT_CH_RETURN_ID = "SELECT leads_id,leads_client_id FROM leads WHERE leads_chcode = (%s) LIMIT 1"
# SELECT_STATUS_RETURN_ID = "SELECT leads_status_id FROM leads_status WHERE leads_status_client_id = (%s) AND leads_status_name = (%s) AND leads_status_deleted = 0  LIMIT 1"
# SELECT_SUBSTATUS_RETURN_ID = "SELECT leads_substatus_id FROM leads_substatus WHERE leads_substatus_status_id = (%s) AND leads_substatus_name = (%s) AND leads_substatus_deleted = 0 LIMIT 1"
# SELECT_AGENT_RETURN_ID = "SELECT users_id FROM users WHERE users_username = (%s) ORDER BY users_id DESC LIMIT 1  "
# INSERT_FIELD_STATUS_RETURN_ID = "INSERT INTO nestform (chcode,reference,bank,status,json_data,area,field_name,date_created) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"
# SELECT_RDS_DATABASE_RETURN_RDS = "SELECT rds_db_name FROM campaigns WHERE campaign_id = (%s) LIMIT 1"
# DELETE_ACCOUNT_RETURN_ID = "UPDATE LEADS SET leads_deleted = 1 where leads_chcode = (%s)"
# SELECT_BANK_NAME = "SELECT db_name,client_id FROM sources WHERE bank_name = %s"

# app = Flask(__name__)


# @api_blueprint.route('/field_status', methods=['POST'])
# @token_required
# @field_required
# def create_field_status(current_user):
#     data = request.get_json()

#     main_db_connection = main_db.get_conn()

#     try:
#         ch_code = data["chcode"]
#         reference = data["reference"]
#         bank = data["bank"]
#         campaign = data["campaign"]
#         status = data["status"]
#         area = data["area"]
#         field_name = data["status"]
#         json_data = data["json_data"]
#         date_created = data["date_created"]

#     # find bank DB and Agent ID in main database
#         with main_db_connection:

#             with main_db_connection.cursor() as cursor:

#                 # Find Database
#                 cursor.execute(SELECT_RDS_DATABASE_RETURN_RDS, (campaign,))
#                 row = cursor.fetchone()
#                 if row != None:

#                     bank_db_name = row[0]

#                 else:
#                     main_db.put_conn(main_db_connection)

#                     return {"error": "Client Does Not Exist"}, 404

#         # connect to specific bank_db_name
#         bank_db = AWSConnection(bank_db_name)
#         connection = bank_db.get_conn()

#         with connection:

#             with connection.cursor() as cursor:
#                 # Insert Field Status

#                 cursor.execute(INSERT_FIELD_STATUS_RETURN_ID, (ch_code, reference,
#                                bank, status, json_data, area, field_name, date_created))
#                 row = cursor.fetchone()
#                 if row != None:

#                     result_id = row[0]

#                 else:
#                     bank_db.put_conn(connection)

#                     return {"error": "Error Inserting Data"}, 404

#         bank_db.put_conn(connection)

#         return {"id": result_id, "message": f"field status {ch_code} created."}, 201

#     except Exception as ex:
#         bank_db.put_conn(connection)

#         return {"error": f"{ex}"}, 400


# @api_blueprint.route('/pullout_account', methods=['POST'])
# @token_required
# @account_etl_required
# def pullout_account(current_user):
#     try:
#         data = request.get_json()
#         ch_code = data['ch_code']
#         bank_name = data['bank_name']
#         main_db_connection = main_db.get_conn()

#         with main_db_connection:

#             with main_db_connection.cursor() as cursor:
#                 cursor.execute(SELECT_BANK_NAME, (bank_name,))
#                 row = cursor.fetchone()
#                 if row != None:

#                     bank_db_name = row[0]

#                 else:
#                     main_db.put_conn(main_db_connection)

#                     return {"error": "Bank name does not exist"}, 404

#                 # connect to specific bank_db_name
#                 bank_db = AWSConnection(bank_db_name)
#                 connection = bank_db.get_conn()
#                 with connection:

#                     with connection.cursor() as cursor:
#                         # Find Client ID

#                         cursor.execute(DELETE_ACCOUNT_RETURN_ID, (ch_code,))
#                         row = cursor.fetchone()

#                         return {"id": row, "message": f"status {ch_code} created."}, 201

#     except Exception as ex:
#         return {"error": f"{ex}"}, 400


# # @api_blueprint.route('/create_agent', methods=['POST', 'GET'])
# # @token_required
# # @itd_required
# # def create_agent(current_user):
# #     data = request.get_json()

# #     main_db_connection = connect_to_main_database()

# #     try:
# #         address = data["address"]
# #         ch_code = data["ch_code"]
# #         contact = data["contact"]
# #         source = data["source"]
# #         start_date = data["start_date"]
# #         end_date = data["end_date"]
# #         amount = data["amount"]
# #         or_number = data["or_number"]
# #         comment = data["comment"]
# #         agent = data["agent"]
# #         disposition_class = data["disposition_class"].split('-')[0].strip()
# #         disposition_code = data["disposition_code"].split('-')[0].strip()
# #         barcode_date_with_tz = datetime.datetime.utcnow()
# #         barcode_date = barcode_date_with_tz.replace(tzinfo=None)

# #         with main_db_connection:

# #             with main_db_connection.cursor() as cursor:


# #                 # Find Agent ID
# #                 cursor.execute(SELECT_AGENT_RETURN_ID,(agent,))
# #                 row = cursor.fetchone()
# #                 if row != None:
# #                     agent_id = row[0]
# #                 else:
# #                     return {"error": "Agent Does Not Exist"},404


# #                 cursor.execute(INSERT_STATUS_RETURN_ID,(leads_id,address,contact,source,start_date,end_date,amount,or_number,comment,agent_id,leads_status_id,leads_substatus_id,barcode_date))
# #                 row = cursor.fetchone()
# #                 if row != None:
# #                     result_id = row[0]

# #                 else:
# #                     main_db.put_conn(main_db_connection)

# #                     return {"error": "Error Inserting Data"},404


# #         main_db.put_conn(main_db_connection)

# #         return {"id": result_id, "message": f"status {ch_code} created."},201

# #     except Exception as ex:

# #         main_db.put_conn(main_db_connection)

# #         return {"error": f"{ex}"},400


# # if __name__ == ('__main__'):

# #     app.run(host='0.0.0.0')

# app.register_blueprint(api_blueprint)
