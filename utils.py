import os
import json
import time
import pandas as pd
import asyncio
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError

from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import (
    Source_System,
    Additional_Interest,
    Additional_Interest_Type,
    broker_portal_error_log
)
from df_database_models.db_utils import (
    generate_uuid, convert_timestamps, query_update_dict, get_record, multi_filter_get_record
)
from secrets_manager import get_secret
from adf_pyutils.clm_wrapper import common_logger

# --- Env DB Configs ---
pnc_db = os.environ.get('RDS_DB_NAME')
ref_db = os.environ.get('RDS_REF_DB_NAME')
mdm_raw_db = os.environ.get('RDS_RAW_DB_NAME')
mdm_refined_db = os.environ.get('RDS_REFINED_DB_NAME')


# --- Async Logger Wrapper ---
async def log_msg(func, **kwargs):
    await asyncio.to_thread(func, **kwargs)


# --- Session/Engine Initializer ---
def call_session_engine(source_system=None, database_name=None):
    rds_secret_name = os.environ["RDS_SECRETS_MANAGER_ID"]
    region_name = os.environ["AWS_REGION"]
    rds_host_nm = os.environ['RDS_HOST']

    if database_name == 'ref_data':
        rds_db_nm = os.environ['RDS_REF_DB_NAME']
    elif database_name == 'mdm_raw':
        rds_db_nm = os.environ['RDS_RAW_DB_NAME']
    elif database_name == 'mdm_refined':
        rds_db_nm = os.environ['RDS_REFINED_DB_NAME']
    else:
        rds_db_nm = os.environ['RDS_DB_NAME']

    # Return AS400 engine per source_system when requested
    if source_system and source_system.lower() == 'as400_aff':
        as400_secret_name = os.environ.get("AS400_AFF_SECRETS_MANAGER_ID") or os.environ.get("as400_aff")
        as400_engine = get_as400_db_session(as400_secret_name, region_name)
        return as400_engine

    elif source_system and source_system.lower() == 'as400_aum':
        as400_secret_name = os.environ.get("AS400_AUM_SECRETS_MANAGER_ID") or os.environ.get("as400_aum")
        as400_engine = get_as400_db_session(as400_secret_name, region_name)
        return as400_engine

    # Return RDS session when database_name requested
    if database_name:
        session = get_rds_db_session(rds_secret_name, region_name, rds_host_nm, rds_db_nm)
        return session


# --- Global Sessions / Engines ---
session = call_session_engine(database_name=pnc_db)
as400_engine_aff = call_session_engine(source_system='as400_aff')
as400_engine_aum = call_session_engine(source_system='as400_aum')


# --- Lookup Additional Interest from AS400 ---
def lookup_as400_additional_interest(config=None, id=None):
    source_system = config.get("source_system") if isinstance(config, dict) else None
    df = None

    # Example query for AFF AS400 - adapt table/schema names as required by real AS400 schema
    if source_system and source_system.lower() == "as400_aff":
        df = pd.read_sql(f"""
            SELECT DISTINCT
                AI.ADDL_INT_ID AS source_additional_interest_id,
                AI.ADDL_INT_NAME             AS name,
                AI.ADDL_INT_TYPE             AS source_additional_interest_type,
                AI.ADDL_INT_ADDL1            AS address_line1,
                AI.ADDL_INT_ADDL2            AS address_line2,
                AI.ADDL_INT_CITY             AS city,
                AI.ADDL_INT_STATE            AS state,
                AI.ADDL_INT_POSTAL           AS postal_code,
                AI.CREATED_DATE              AS created_date,
                AI.MODIFIED_DATE             AS modified_date
            FROM ADGDTAPR.ADDLINT AI
            WHERE AI.ADDL_INT_ID = '{id}'
        """, con=as400_engine_aff)

    elif source_system and source_system.lower() == "as400_aum":
        # Example query for AUM AS400; adjust to actual schema
        df = pd.read_sql(f"""
            SELECT
                ADDL_ID AS source_additional_interest_id,
                NAME    AS name,
                TYPE    AS source_additional_interest_type,
                ADDR1   AS address_line1,
                ADDR2   AS address_line2,
                CITY    AS city,
                ST      AS state,
                ZIP     AS postal_code,
                CREATED_AT AS created_date,
                UPDATED_AT AS modified_date
            FROM AUMDB.ADDITIONAL_INTEREST
            WHERE ADDL_ID = '{id}'
        """, con=as400_engine_aum)

    if df is not None and len(df) > 0:
        return df.to_dict("records")[0]
    return None


# --- Core Lambda Consumer ---
async def consume_lambda(config=None):
    asyncio.create_task(log_msg(common_logger, log_messages="consume additional interest lambda invoking"))
    now = datetime.now()
    start_timestamp = datetime.timestamp(now)
    asyncio.create_task(log_msg(common_logger, log_messages=f"Processing to DB @ {now} | {start_timestamp}"))

    try:
        asyncio.create_task(log_msg(common_logger, log_messages="Config", api_response=convert_timestamps(config)))
        config_dicts = config if isinstance(config, dict) else json.loads(str(config))
        if not isinstance(config_dicts, list):
            config_dicts = [config_dicts]

        for config_dict in config_dicts:
            ai_id = config_dict.get("Additional_Interest") or config_dict.get("additional_interest") or config_dict.get("addl_interest")
            source_system = (config_dict.get("source_system") or "").lower()

            # choose correct as400 engine for source system
            global as400_engine
            if source_system == "as400_aff":
                as400_engine = as400_engine_aff
            elif source_system == "as400_aum":
                as400_engine = as400_engine_aum
            else:
                as400_engine = None

            # Lookup from AS400
            as400_ai_dict = lookup_as400_additional_interest(config_dict, ai_id)

            if as400_ai_dict:
                asyncio.create_task(log_msg(common_logger, log_messages=f"Initial {source_system} Additional Interest dict:", api_response=convert_timestamps(as400_ai_dict)))

                # --- Map Source System ---
                source_system_record = (
                    query.first() if (query := multi_filter_get_record(session, model=Source_System, source_system=source_system)) is not None else None
                )
                if source_system_record:
                    as400_ai_dict["df_source_system_id"] = source_system_record.df_source_system_id

                # --- Map Additional Interest Type ---
                ai_type_record = (query.first() if (query := get_record(session, model=Additional_Interest_Type, column_name='source_additional_interest_type', value=as400_ai_dict.get('source_additional_interest_type'))) is not None else None)
                if ai_type_record:
                    as400_ai_dict["df_additional_interest_type_id"] = ai_type_record.df_additional_interest_type_id

                # --- Determine df_additional_interest_id (insert vs update) ---
                ai_record_q = get_record(session, model=Additional_Interest, column_name='df_additional_interest_id', value=ai_id)
                ai_record = ai_record_q.first() if ai_record_q else None

                if not ai_record:
                    # create new df_additional_interest_id
                    as400_ai_dict["df_additional_interest_id"] = generate_uuid(str(ai_id), as400_ai_dict.get("df_source_system_id"))
                    session.add(Additional_Interest.from_dict(cls=Additional_Interest, d=as400_ai_dict))
                    session.commit()
                    asyncio.create_task(log_msg(common_logger, log_messages=f"Inserted Additional Interest {ai_id}"))
                else:
                    # update existing
                    ai_record.update(query_update_dict(obj=Additional_Interest, dict=as400_ai_dict))
                    session.commit()
                    asyncio.create_task(log_msg(common_logger, log_messages=f"Updated Additional Interest {ai_id}"))

            else:
                error_log = {"df_additional_interest_id": ai_id, "error_message": "No record found after lookup"}
                asyncio.create_task(log_msg(common_logger, log_messages=f"No record found for Additional Interest {ai_id}", api_response=error_log))

        end_timestamp = datetime.timestamp(datetime.now())
        asyncio.create_task(log_msg(common_logger, log_messages=f"execution_time: {end_timestamp - start_timestamp}"))

    except SQLAlchemyError as e:
        session.rollback()
        asyncio.create_task(log_msg(common_logger, log_messages="DB Error", api_response=str(e)))
        raise e


# --- Lambda Handler ---
def handle(event, context):
    start_time = time.time()
    print("Handle function is called")
    for record in event['Records']:
        payload = record["body"]
        asyncio.run(consume_lambda(config=payload))
    return {"execution_time_sec": time.time() - start_time}


if __name__ == "__main__":
    handle({"Records": [{"body": '{ "Additional_Interest": "AI12345", "source_system": "as400_aff", "name": "Mortgage Holder" }'}]}, None)
