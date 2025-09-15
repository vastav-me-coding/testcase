import os
import json
import time
import asyncio
import pandas as pd
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError

from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import Carrier, Source_System
from df_database_models.db_utils import generate_uuid, query_update_dict, get_record, convert_timestamps
from adf_pyutils.clm_wrapper import common_logger
from secrets_manager import get_secret

print("AS400 Carrier ETL job executing...")

# --- AWS RDS Connection ---
rds_secret_name = os.environ["RDS_SECRETS_MANAGER_ID"]
region_name = os.environ["AWS_REGION"]
rds_host = os.environ["RDS_HOST"]
rds_db = os.environ["RDS_DB_NAME"]

session = get_rds_db_session(rds_secret_name, region_name, rds_host, rds_db)

async def log_msg(func, **kwargs):
    await asyncio.to_thread(func, **kwargs)
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

# --- Lookup Carrier from AS400 ---
def lookup_as400_carrier(config=None, id=None):
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

# --- Main Consumer Function ---
async def consume_lambda(config=None):
    try:
        asyncio.create_task(log_msg(common_logger,log_messages='consume lambda function invoking'))
        now = datetime.now()
        start_timestamp = datetime.timestamp(now)
        asyncio.create_task(log_msg(common_logger,log_messages=f'Processing to DB @ {now} | {datetime.timestamp(now)}'))
        asyncio.create_task(log_msg(common_logger,log_messages='Config',api_response=convert_timestamps(config)))
        config_dicts = config if type(config) is dict else json.loads(str(config))
        if type(config_dicts) == list:
            pass
        else:
            config_dicts = [config_dicts] # Ensure config_dicts is a list
        for config_dict in config_dicts:
            carrier_id = config_dict["carrier"]
            source_system = config_dict["source_system"]
            global session, as400_engine
            session, as400_engine = call_session_engine(source_system=source_system)

            asyncio.create_task(log_msg(common_logger, log_messages=f"Fetching carrier {carrier_id} from {source_system}"))

            as400_carrier = lookup_as400_carrier(carrier_id, source_system)
            if as400_carrier:
                # --- Map Source System ---
                source_system_record = (query.first() if (query := get_record(
                    session, model=Source_System, column_name="source_system", value=source_system)) is not None else None)
                if source_system_record:
                    as400_carrier["df_source_system_id"] = source_system_record.df_source_system_id

                # --- Carrier ID ---
                df_carrier_id = generate_uuid(str(as400_carrier["source_carrier_id"]), as400_carrier["df_source_system_id"])
                as400_carrier["df_carrier_id"] = df_carrier_id

                # --- Check if Carrier already exists ---
                carrier_record = get_record(session, model=Carrier,
                                            column_name="source_carrier_id",
                                            value=as400_carrier["source_carrier_id"],
                                            df_source_system_id=as400_carrier["df_source_system_id"])
                self_carrier = carrier_record.first() if carrier_record else None

                if self_carrier is None:
                    session.add(Carrier.from_dict(cls=Carrier, d=as400_carrier))
                    asyncio.create_task(log_msg(common_logger, log_messages=f"Inserted new Carrier {carrier_id}"))
                else:
                    self_carrier.update(query_update_dict(obj=Carrier, dict=as400_carrier))
                    asyncio.create_task(log_msg(common_logger, log_messages=f"Updated Carrier {carrier_id}"))

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
        session.rollback()
        asyncio.create_task(log_msg(common_logger, log_messages="DB Error", api_response=str(e)))
        raise e

def handle(event, context):
    start_time = time.time()
    for record in event["Records"]:
        payload = record["body"]
        asyncio.run(consume_lambda(config=payload))
    return {"execution_time_sec": time.time() - start_time}
