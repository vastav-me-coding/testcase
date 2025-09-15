import os
import json
import time
import requests
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import Source_System, Line_Item, Line_Item_Type, Invoice, broker_portal_error_log, Customer, Customer_Contact
from df_database_models.db_utils import  generate_uuid, convert_timestamps, generate_uuid, query_update_dict, get_record, call_sp
from secrets_manager import get_secret
from datetime import datetime
import pandas as pd
import asyncio
from adf_pyutils.clm_wrapper import common_logger

print("Class job is executing")

## Fetch SQS producer Parameters from aws secret manager
# sqs_producer_secret = json.loads(get_secret(
#          secret_name=os.environ["SQS_PRODUCER_SECRET_ID"], region_name=os.environ["AWS_REGION"]))
# sqs_producer_access_key = sqs_producer_secret["access_key"]
# sqs_policy_update_url = os.environ["SQS_POLICY_UPDATE_URL"]

async def log_msg(func,**kwargs):
    await asyncio.to_thread(func,**kwargs)

def call_session_engine(source_system=None, identifier=None):

    if source_system:
        rds_secret_name=os.environ["RDS_SECRETS_MANAGER_ID"]
        region_name=os.environ["AWS_REGION"]
        rds_host_nm=os.environ['RDS_HOST']

        if identifier == 'ref':
            rds_db_nm=os.environ['RDS_REF_DB_NAME']
        elif identifier == 'raw':
            rds_db_nm=os.environ['RDS_RAW_DB_NAME']
        elif identifier == 'refined':
            rds_db_nm=os.environ['RDS_REFINED_DB_NAME']
        else:
            rds_db_nm=os.environ['RDS_DB_NAME']

        if source_system.lower() == '':#Type of system to enter is as400 has types
            #Calling the as400 engine to establish a connection to PAS Source System - AS400

            as400_secret_name=os.environ["as400_aff"]#enter secret manager id
            as400_engine=get_as400_db_session(as400_secret_name, region_name)

        elif source_system.lower() == '':
            #Calling the as400 engine to establish a connection to PAS Source System - AS400
            as400_secret_name=os.environ["as400_aum"]#enter secret manager id
            as400_engine=get_as400_db_session(as400_secret_name, region_name)

        #Calling the Db session Object to establish a connection to Data Foundation Schema
        session=get_rds_db_session(rds_secret_name,region_name,rds_host_nm,rds_db_nm)

        return session, as400_engine

# lookup into aumine aff: read data from lineitem table in Aumine 
# lookup into aumine aff
def lookup_as400(config=None, id=None):
    source_system = config['source_system']
    if source_system:
        if source_system.lower() in []:# Add source system
            df = pd.read_sql(f"""
                    #Add Query 
                    """, con=as400_engine)
        else:
            df=None
    else:
        df=None

    if(len(df)>0):
        return df.to_dict('records')[0]
    else:
        return None

# Main function to process incoming configuration data
async def consume_lambda(config=None):
    asyncio.create_task(log_msg(common_logger,log_messages='consume lambda function invoking'))
    now = datetime.now()
    start_timestamp = datetime.timestamp(now)
    asyncio.create_task(log_msg(common_logger,log_messages=f'Processing to DB @ {now} | {datetime.timestamp(now)}'))

    try:
        asyncio.create_task(log_msg(common_logger,log_messages='Config',api_response=convert_timestamps(config)))
        config_dicts = config if type(config) is dict else json.loads(str(config))
        if type(config_dicts) == list:
            pass
        else:
            config_dicts = [config_dicts] # Ensure config_dicts is a list
        for config_dict in config_dicts:
            customer_id = config_dict['Customer'] # Get the lineitem from config data 
            source_system = config_dict['source_system'].lower()
            if(id):
                fk_flag = 1
                print("Calling call_session_engine Function")
                global session, as400_engine
                session, as400_engine = call_session_engine(source_system=source_system)

                as400_customer_summary_dict = lookup_as400(config_dict, customer_id) #define dict for lookup data
                
                if(as400_customer_summary_dict):
                    asyncio.create_task(log_msg(common_logger,log_messages=f'Initial {source_system} Customer Summary dict:',api_response=convert_timestamps(as400_customer_summary_dict)))

                    #Fetch Source SyStem Id from Data Foundation
                    # source_system = as400_customer_summary_dict.get("source_system")
                    source_system_record = (query.first() if (query := get_record(session,model=Source_System,column_name='source_system',value=source_system)) is not None else None)
                    if source_system_record:
                        as400_customer_summary_dict['df_source_system_id'] = source_system_record.df_source_system_id

                    #Fetch LineItem Type Id from Data Foundation
                    # line_item_type_record = as400_customer_summary_dict.get("line_item_type")
                    customer_record = (query.first() if (query := get_record(session,model=Line_Item_Type,column_name='source_customer_id',value = customer_id)) is not None else None)
                    self_customer = (customer_record.first() if customer_record is not None else None)
                    
                    if self_customer is None:
                        asyncio.create_task(log_msg(common_logger,log_messages='Customer does not exist in Data Foundation'))
                        as400_customer_summary_dict['df_customer_id'] = generate_uuid(
                            str(as400_customer_summary_dict['source_customer_id'] or '') + 
                            str(as400_customer_summary_dict['df_source_system_id'] or ''), 
                        )
                        session.add(Customer.from_dict(cls = Customer, d = as400_customer_summary_dict))
                        session.commit()
                        fk_flag = 0
                        asyncio.create_task(log_msg(common_logger,log_messages='Inserted Customer {customer_id}'))
                    else:
                        asyncio.create_task(log_msg(common_logger,log_messages='Customer exists in Data Foundation'))
                        customer_record.update(query_update_dict( obj = Customer, dict = as400_customer_summary_dict))
                        session.commit()

                    error_log = {
                        "df_line_item_id" : id,
                        "error_message" : "No record found after lookup" 
                    }
                    asyncio.create_task(log_msg(common_logger,log_messages='No record found after lookup'))
        now = datetime.now()
        end_timestamp = datetime.timestamp(now)
        asyncio.create_task(log_msg(common_logger,log_messages=f'execution_time: {end_timestamp} - {start_timestamp}'))
    except SQLAlchemyError as e:
        asyncio.create_task(log_msg(common_logger,log_messages='Error',api_response=e))
        session.rollback()
        raise e


def handle(event, context):
    start_time = time.time()
    print("Handle function is called")
    for record in event['Records']:
        payload = record["body"]
        asyncio.run(consume_lambda(config=payload))
    end_time = time.time()
    return {
        "execution_time_sec": end_time - start_time 
    }

if __name__ == '__main__':
    handle({"Records": [{"body": '{ "Customer": "CUST12345", "source_system": "as400" "customer_name": "John Doe", "customer_type": "Retail" }'}]}, None)
    