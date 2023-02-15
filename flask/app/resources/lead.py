import datetime
from flask_restful import Resource, request
from app.utils import account_etl_required, token_required, agent_status_required
from app.etl_upsert import ETL
from app.databases.aws import AWSConnection, main_db
SELECT_RDS_DATABASE_RETURN_RDS = "SELECT rds_db_name FROM campaigns WHERE campaign_id = (%s) LIMIT 1"
SELECT_AGENT_RETURN_ID = "SELECT users_id FROM users WHERE users_username = (%s) ORDER BY users_id DESC LIMIT 1  "
SELECT_CH_RETURN_ID = "SELECT leads_id,leads_client_id FROM leads WHERE leads_chcode = (%s) LIMIT 1"
SELECT_STATUS_RETURN_ID = "SELECT leads_status_id FROM leads_status WHERE leads_status_client_id = (%s) AND leads_status_name = (%s) AND leads_status_deleted = 0  LIMIT 1"
SELECT_SUBSTATUS_RETURN_ID = "SELECT leads_substatus_id FROM leads_substatus WHERE leads_substatus_status_id = (%s) AND leads_substatus_name = (%s) AND leads_substatus_deleted = 0 LIMIT 1"
INSERT_STATUS_RETURN_ID = "INSERT INTO leads_result (leads_result_lead,leads_result_address,leads_result_contact,leads_result_source,leads_result_sdate,leads_result_edate,leads_result_amount,leads_result_ornumber,leads_result_comment,leads_result_users,leads_result_status_id,leads_result_substatus_id,leads_result_barcode_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING leads_result_id;"


class LeadResource(Resource):
    @token_required
    @account_etl_required
    def post(self, current_user):
        bank_name = request.json['bank_name']

        # Create an ETL object
        etl = ETL()

        # Call the replicate() method
        etl.replicate(bank_name)

        return {"id": bank_name, "message": f"successfully sync"}, 200


class LeadResultResource(Resource):
    def put(self, id):
        return {'message': 'create', 'data': id}, 200

    @token_required
    @agent_status_required
    def post(self, current_user):

        data = request.get_json()

        main_db_connection = main_db.get_conn()

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
            with main_db_connection:

                with main_db_connection.cursor() as cursor:

                    # Find Database
                    cursor.execute(SELECT_RDS_DATABASE_RETURN_RDS, (campaign,))
                    row = cursor.fetchone()

                    if row != None:

                        bank_db_name = row[0]

                    else:
                        main_db.put_conn(main_db_connection)

                        return {"error": "Client Does Not Exist"}, 404

                    # Find Agent ID
                    cursor.execute(SELECT_AGENT_RETURN_ID, (agent,))
                    row = cursor.fetchone()

                    if row != None:
                        agent_id = row[0]
                    else:

                        # main_db.put_conn(connection)

                        return {"error": "Agent Does Not Exist"}, 404

            # connect to specific bank_db_name
            bank_db = AWSConnection(bank_db_name)
            connection = bank_db.get_conn()
            with connection:

                with connection.cursor() as cursor:
                    # Find Client ID

                    cursor.execute(SELECT_CH_RETURN_ID, (ch_code,))
                    row = cursor.fetchone()

                    if row != None:

                        leads_id = row[0]
                        leads_client_id = row[1]

                    else:

                        bank_db.put_conn(connection)

                        return {"error": "Client Does Not Exist"}, 404

                    # Find Status
                    cursor.execute(SELECT_STATUS_RETURN_ID,
                                   (leads_client_id, disposition_class,))
                    row = cursor.fetchone()
                    print(ch_code)
                    print(row)
                    if row != None:
                        leads_status_id = row[0]
                    else:

                        bank_db.put_conn(connection)

                        return {"error": "Status Does Not Exist"}, 404

                    cursor.execute(SELECT_SUBSTATUS_RETURN_ID,
                                   (leads_status_id, disposition_code,))
                    row = cursor.fetchone()
                    if row != None:
                        leads_substatus_id = row[0]
                    else:
                        bank_db.put_conn(connection)

                        return {"error": "Sub Status Does Not Exist"}, 404

                    cursor.execute(INSERT_STATUS_RETURN_ID, (leads_id, address, contact, source, start_date, end_date,
                                                             amount, or_number, comment, agent_id, leads_status_id, leads_substatus_id, barcode_date))
                    row = cursor.fetchone()

                    if row != None:
                        result_id = row[0]

                    else:
                        bank_db.put_conn(connection)

                        return {"error": "Error Inserting Data"}, 404

            return {"id": result_id, "message": f"status {ch_code} created."}, 201

        except Exception as ex:
            main_db.put_conn(main_db_connection)

            bank_db.put_conn(connection)

            return {"error": f"{ex}"}, 400