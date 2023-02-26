import datetime
from flask_restful import Resource, request
from app.utils import account_etl_required, token_required, agent_status_required
from app.etl_upsert import ETL
from app.databases.aws import AWSConnectionPool, main_db
SELECT_RDS_DATABASE_RETURN_RDS = "SELECT rds_db_name FROM campaigns WHERE campaign_id = (%s) LIMIT 1"
SELECT_AGENT_RETURN_ID = "SELECT users_id FROM users WHERE users_username = (%s) ORDER BY users_id DESC LIMIT 1  "
SELECT_CH_RETURN_ID = "SELECT leads_id,leads_client_id FROM leads WHERE leads_chcode = (%s) LIMIT 1"
SELECT_STATUS_RETURN_ID = "SELECT leads_status_id FROM leads_status WHERE leads_status_client_id = (%s) AND leads_status_name = (%s) AND leads_status_deleted = 0  LIMIT 1"
SELECT_SUBSTATUS_RETURN_ID = "SELECT leads_substatus_id FROM leads_substatus WHERE leads_substatus_status_id = (%s) AND leads_substatus_name = (%s) AND leads_substatus_deleted = 0 LIMIT 1"
INSERT_STATUS_RETURN_ID = "INSERT INTO leads_result (leads_result_lead,leads_result_address,leads_result_contact,leads_result_source,leads_result_sdate,leads_result_edate,leads_result_amount,leads_result_ornumber,leads_result_comment,leads_result_users,leads_result_status_id,leads_result_substatus_id,leads_result_barcode_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING leads_result_id;"
class HttpException(Exception):
    def __init__(self, status, message, data=None):
        self.status = status
        self.message = message
        self.data = data
from app.services.lead import ETLLead

class LeadResource(Resource):
    @token_required
    @account_etl_required
    def post(self, current_user):
        try:
            compain_name = request.json['compain_name']
            # create etl lead instance
            etl_lead = ETLLead(compain_name)

            # Call the replicate() method
            etl_lead.replicate()

            return {"id": compain_name, "message": f"successfully sync"}, 200
        except Exception as ex:
            print(ex)
            return {"error": f"{ex}"}, 400



class LeadPulloutResouce(Resource):
    @token_required
    @account_etl_required
    def put(self, current_user):
        if 'db_name' not in request.json or 'ch_code' not in request.json:
            return {'message': 'db_name and ch_code required.'}, 400

        db_name = request.json['db_name']
        ch_code = request.json['ch_code']

        try:
            with main_db as main_db_conn:
                cursor = main_db_conn.cursor()
                cursor.execute(
                    'SELECT db_name from api_db_destinations WHERE db_name = %s', (db_name,))
                row = cursor.fetchone()
                if row == None:
                    return {'message': 'Campaign name is not found'}, 400
                db_bank_name = row[0]

                bank_db = AWSConnectionPool(db_bank_name)
                
                with bank_db as bank_db_conn:
                    bank_db_cursor = bank_db_conn.cursor()

                    bank_db_cursor.execute(
                        'UPDATE leads SET leads_deleted = %s, leads_pullout = %s WHERE leads_chcode = %s RETURNING leads_chcode', (1, datetime.datetime.utcnow(), ch_code))
                    bank_db_conn.commit()
                    row = bank_db_cursor.fetchone()

                    if row == None:
                        return {'message': 'Invalid ch_code'}, 400
                    updated_lead_ch_code = row[0]

                    return {'message': f'leads with ch_code of {updated_lead_ch_code} has been deleted'}
        
        except Exception as ex:
            print(ex)
            return {"error": f"{ex}"}, 400


class LeadResultResource(Resource):
    def put(self, id):
        return {'message': 'create', 'data': id}, 200

    @token_required
    @agent_status_required
    def post(self, current_user):

        data = request.get_json()

        
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

            if (len(start_date) < 3):
                start_date = None

            if (len(end_date) < 3):
                end_date = None

            if (len(amount) < 2):
                amount = 0

            # find bank DB and Agent ID in main database
            with main_db as main_db_connection:

                cursor = main_db_connection.cursor()
                # Find Database
                cursor.execute(SELECT_RDS_DATABASE_RETURN_RDS, (campaign,))
                row = cursor.fetchone()

                if row != None:

                    bank_db_name = row[0]

                else:
                    return {"error": "Client Does Not Exist"}, 404

                # Find Agent ID
                cursor.execute(SELECT_AGENT_RETURN_ID, (agent,))
                row = cursor.fetchone()

                if row != None:
                    agent_id = row[0]
                else:
                    return {"error": "Agent Does Not Exist"}, 404
                
            # connect to specific bank_db_name
            bank_db = AWSConnectionPool(bank_db_name)
            with bank_db as bank_db_conn:

                cursor = bank_db_conn.cursor()
                # Find Client ID

                cursor.execute(SELECT_CH_RETURN_ID, (ch_code,))
                row = cursor.fetchone()

                if row != None:

                    leads_id = row[0]
                    leads_client_id = row[1]

                else:

                    return {"error": "Client Does Not Exist"}, 404

                # Find Status
                cursor.execute(SELECT_STATUS_RETURN_ID,
                               (leads_client_id, disposition_class,))
                row = cursor.fetchone()
                if row != None:
                    leads_status_id = row[0]
                else:
                    return {"error": "Status Does Not Exist"}, 404

                cursor.execute(SELECT_SUBSTATUS_RETURN_ID,
                               (leads_status_id, disposition_code,))
                row = cursor.fetchone()
                if row != None:
                    leads_substatus_id = row[0]
                else:
                    return {"error": "Sub Status Does Not Exist"}, 404

                cursor.execute(INSERT_STATUS_RETURN_ID, (leads_id, address, contact, source, start_date, end_date,
                                                         amount, or_number, comment, agent_id, leads_status_id, leads_substatus_id, barcode_date))
                bank_db_conn.commit()
                row = cursor.fetchone()
                if row != None:
                    result_id = row[0]

                else:
                    
                    return {"error": "Error Inserting Data"}, 404

            return {"id": result_id, "message": f"status {ch_code} created."}, 201

        except Exception as ex:
            print(ex)
            return {"error": f"{ex}"}, 400
