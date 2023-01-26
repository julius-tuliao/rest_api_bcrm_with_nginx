import configparser
import psycopg2
import psycopg2.extras
import psycopg2.pool
import json
from datetime import datetime
import dotenv 
import os




class ETL:
    def __init__(self):
        # Parse the configuration file

        dotenv.load_dotenv('dev.env')

        # Read the connection details from the configuration file
        self.source_host = os.getenv("AMEYO_DATABASE_HOST")
        self.source_user = os.getenv("AMEYO_DATABASE_USER")
        self.source_password = os.getenv("AMEYO_DATABASE_PASSWORD")
        self.destination_host = os.getenv("RDS_DATABASE_HOST")
        self.destination_db = os.getenv("RDS_DATABASE_NAME")
        self.destination_user = os.getenv("RDS_DATABASE_USER")
        self.destination_password = os.getenv("RDS_DATABASE_PASSWORD")

    def replicate(self, bank_name):

        
        # QUERIES
        SELECT_BANK_NAME = "SELECT db_name,client_id FROM sources WHERE bank_name = %s"
        SELECT_AMEYO_RECORDS = "SELECT ch_code,ch_name,account_number,principal,endorsement_date,\
                cutoff_date,address1,address2,address3,address4,address5,phone1,phone2,phone3,new_contact,\
                new_email_address,agent,date_added,account_information,additional_information FROM customer \
                Where LENGTH(ch_code) > 0 AND LENGTH(ch_name) > 0 AND LENGTH(outstanding_balance) > 0 AND LENGTH(account_number) > 0 AND (updatedon::date > current_date - interval '24 HOURS' or date_added > current_date - interval '24 HOURS') "
        SELECT_USER_ID = "SELECT users_id FROM users WHERE users_username = %s"
        SELECT_IF_ROW_EXIST = "SELECT leads_chcode FROM leads WHERE leads_chcode = %s"
        INSERT_ETL_LEADS = "INSERT INTO public.leads ( leads_client_id, leads_users_id, \
                            leads_acctno, leads_chcode, leads_chname, leads_prin, leads_interest, \
                            leads_endo_date, leads_lpd, leads_lpa, leads_cycle, \
                            leads_credit_limit, leads_type_product, leads_work_phone, leads_ob, \
                            leads_new_ob, leads_full_address, leads_address1, leads_address2, leads_address3,\
                            leads_address4,leads_phone, leads_email, leads_employer, leads_birthday,\
                            leads_mobile_phone, leads_cutoff, leads_new_cutoff, leads_full_saddress, leads_full_taddress, \
                            leads_new_address, leads_new_contact, leads_spo,leads_ts,leads_placement) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING leads_id"
        
        UPDATE_ETL_LEADS = "UPDATE public.leads SET leads_client_id=%s, leads_users_id=%s, \
                            leads_acctno=%s, leads_chcode=%s, leads_chname=%s, leads_prin=%s, leads_interest=%s, \
                            leads_endo_date=%s, leads_lpd=%s, leads_lpa=%s, leads_cycle=%s, \
                            leads_credit_limit=%s, leads_type_product=%s, leads_work_phone=%s, leads_ob=%s, \
                            leads_new_ob=%s, leads_full_address=%s, leads_address1=%s, leads_address2=%s, leads_address3=%s,\
                            leads_address4=%s,leads_phone=%s, leads_email=%s, leads_employer=%s, leads_birthday=%s,\
                            leads_mobile_phone=%s, leads_cutoff=%s, leads_new_cutoff=%s, leads_full_saddress=%s, leads_full_taddress=%s, \
                            leads_new_address=%s, leads_new_contact=%s, leads_spo=%s,leads_ts=%s,leads_placement=%s \
                            WHERE leads_chcode=%s RETURNING leads_id"

        SELECT_DYNAMIC_ID = "SELECT dynamic_id FROM dynamic WHERE dynamic_client_id = %s AND dynamic_system_name = %s"

        INSERT_DYNAMIC_ROW = "INSERT INTO public.dynamic_value(dynamic_value_lead_id, dynamic_value_dynamic_id, dynamic_value_name) VALUES (%s, %s, %s) ON CONFLICT (dynamic_value_lead_id, dynamic_value_dynamic_id) DO UPDATE SET dynamic_value_name = EXCLUDED.dynamic_value_name"

        # Connect to the destination database
        conn_destination = psycopg2.pool.SimpleConnectionPool(5, 10,host=self.destination_host, database=self.destination_db, user=self.destination_user, password=self.destination_password)
        conn_destination_pool = conn_destination.getconn()
        cursor_destination = conn_destination_pool.cursor()

        
         # Query the database to retrieve the source database names
        cursor_destination.execute(SELECT_BANK_NAME, (bank_name,))
        results = cursor_destination.fetchall()

       
        # Loop through each source database
        for bank_row in results:

            source_db = bank_row[0]
            client_id = bank_row[1]

            # Connect to the source database
            conn_source = psycopg2.pool.SimpleConnectionPool(5, 10,host=self.source_host, database=source_db, user=self.source_user, password=self.source_password)
            conn_source_pool = conn_source.getconn()
            cursor_source = conn_source_pool.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Execute a SELECT query to retrieve the data
            cursor_source.execute(SELECT_AMEYO_RECORDS)
            data = cursor_source.fetchall()

          

            # filter_complete_data the data (if necessary)
            data = self.filter_complete_data(data)



            # Iterate through the data and perform an upsert operation
            for row in data:
                ch_code = row["ch_code"]
                print(ch_code)
                ch_name = row["ch_name"]
                agent_code = row["agent"]
                acct_number = row["account_number"]
                principal_text = row["principal"]

                account_information_raw = row["account_information"].replace("\\","/")
                additional_information_raw = row["additional_information"].replace("\\","/")

                account_info = json.loads(account_information_raw)
                additional_info = json.loads(additional_information_raw)
                account_info = account_info[0]
                ob_text = account_info.get("OB","")
                principal_text = account_info.get("PRINCIPAL","")
                interest = account_info.get("INTEREST","")
                lpd_text = account_info.get("LPD","")
                lpa_text = account_info.get("LPA","")
                credit_limit_text = account_info.get("CREDIT LIMIT","")
                block_code = account_info.get("BLOCK CODE","")
                employer = account_info.get("EMPLOYER","")
                primary_add = account_info.get("PRIMARY ADDRESS","")
                secondary_add = account_info.get("SECONDARY ADDRESS","")
                tertiary_add = account_info.get("TERTIARY ADDRESS","")
                new_add = account_info.get("NEW ADDRESS","")
                work_phone = account_info.get("WORK PHONE","")
                mobile_phone = account_info.get("MOBILE PHONE","")
                new_contact = account_info.get("NEW CONTACT","")
                birthday_text = account_info.get("BIRTHDAY","")
                new_ob_text = account_info.get("NEW OB","")
                new_cutoff_text = account_info.get("NEW CUTOFF","")
                cycle = account_info.get("CYCLE","")
                product_type = account_info.get("PRODUCT TYPE","")
                email = account_info.get("EMAIL","")
                spo = account_info.get("SPO","")
                placement = account_info.get("PLACEMENT","")
                endo_date_text = row["endorsement_date"]
                address1 = row["address1"]
                address2 = row["address2"]
                address3 = row["address3"]
                address4 = row["address4"]
                address5 = row["address5"]
                phone1 = row["phone1"]
                phone2 = row["phone2"]
                phone3 = row["phone3"]
                date_added = row["date_added"]

                # convert ameyo data to correct format
                endo_date = self.convert_text_to_date(endo_date_text)
                lpd = self.convert_text_to_date(lpd_text)
                new_cutoff = self.convert_text_to_date(new_cutoff_text)
                birthday = self.convert_text_to_date(birthday_text)
                principal = self.remove_non_numeric(principal_text)
                ob = self.remove_non_numeric(ob_text)
                lpa = self.remove_non_numeric(lpa_text)
                credit_limit = self.remove_non_numeric(credit_limit_text)
                new_ob = self.remove_non_numeric(new_ob_text)

                # Retrieve the user ID for the agent name
                cursor_destination.execute( SELECT_USER_ID, (agent_code,))
                user = cursor_destination.fetchone()
               
                if user is not None:
                    user_id = user[0]
                
                    

                    # Check if the row already exists in the table
                    cursor_destination.execute(SELECT_IF_ROW_EXIST, (ch_code,))
                    

                    # Get the current time
                    current_time = datetime.now()

                    # Format the current time as a PostgreSQL timestamp without a timezone
                    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
              
                    if cursor_destination.fetchone() is None:
                        
                        # Row does not exist, perform an INSERT
                        cursor_destination.execute(INSERT_ETL_LEADS, 
                            (client_id,user_id,acct_number,ch_code,ch_name,principal,interest,endo_date,lpd, \
                            lpa, cycle,credit_limit,product_type,work_phone,ob,new_ob,primary_add,address1,address2,address3,address4,\
                            phone1,email,employer,birthday,mobile_phone,new_cutoff,new_cutoff,secondary_add,tertiary_add, \
                            new_add,new_contact,spo,current_time,placement))

                        # Get the ID of the inserted or updated row
                        leads_id = cursor_destination.fetchone()[0]

                        # Iterate through the list
                        # for row in additional_info:
                        #     for key in row.keys():
                        #         # Access the value for each key
                        #         value = row[key]

                        #         if value != None:   

                        #             cursor_destination.execute(SELECT_DYNAMIC_ID,(client_id,key))

                        #             if cursor_destination.rowcount > 0:
                        #                 dynamic_id = cursor_destination.fetchone()[0]

                        #                 cursor_destination.execute(INSERT_ DYNAMIC_ROW,(leads_id,dynamic_id,value))                     

                    
                    else:

                        # Row exists, perform an UPDATE
                        # cursor_destination.execute(f"UPDATE leads SET column1 = %s, column2 = %s WHERE ch_code = %s", (other_columns[0], other_columns[1], ch_code))
                        cursor_destination.execute(UPDATE_ETL_LEADS, 
                            (client_id,user_id,acct_number,ch_code,ch_name,principal,interest,endo_date,lpd, \
                            lpa, cycle,credit_limit,product_type,work_phone,ob,new_ob,primary_add,address1,address2,address3,address4,\
                            phone1,email,employer,birthday,mobile_phone,new_cutoff,new_cutoff,secondary_add,tertiary_add, \
                            new_add,new_contact,spo,current_time,placement, ch_code))


                        # Get the ID of the inserted or updated row
                        leads_id = cursor_destination.fetchone()[0]

                        # Iterate through the keys
                        # for row in additional_info:
                        #     for key in row.keys():
                        #         # Access the value for each key
                        #         value = row[key]

                        #         if value != None:   
                        #             cursor_destination.execute(SELECT_DYNAMIC_ID,(client_id,key))

                                    
                        #             if cursor_destination.rowcount > 0:
                        #                 dynamic_id = cursor_destination.fetchone()[0]

                        #                 cursor_destination.execute(INSERT_DYNAMIC_ROW,(leads_id,dynamic_id,value))                     
    
                        
                
        # Commit the changes to the database
        conn_destination_pool.commit()


        # Close the cursor and connection
        cursor_destination.close()
        conn_destination.closeall()

        # Close the cursor and connection
        cursor_source.close()
        conn_source.closeall()

    def filter_complete_data(self,data):
        # Filter out rows with blank values in the ch_code, ch_name, or account_information columns
        data = [row for row in data if row['ch_code'] and row['ch_name'] and row['account_information'] 
                and str(row['ch_code']).strip() and str(row['ch_name']).strip() 
                and str(row['account_information']).strip()]
        
        return data

    def convert_text_to_date(self, date_text):
        # convert endo in text format to postgres date_text

        if(len(date_text) < 5):
            endo_date = None
        else:
            try:
                # Try to parse the date string using the "mm/dd/yyyy" format
                endo_date = datetime.strptime(date_text, "%m/%d/%Y")
            except ValueError:
                # If the parsing fails, try to parse the date string using another format
                try:
                    endo_date = datetime.strptime(date_text, "%b %d, %Y")
                except ValueError:
                    # If none of the formats work, return the default date
                    endo_date = datetime(1970, 1, 1)
        
        return endo_date

    def remove_non_numeric(self,string):
        
        string = str(string)

        if(len(string)>0):
            return "".join(c for c in string if c.isdigit() or c == ".")

        else:
            return None