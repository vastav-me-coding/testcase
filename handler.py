import os
import logging
import json
import time
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import BIGINT, create_engine, Column, Integer, String, MetaData, DateTime, Float, Text, Date
from sqlalchemy.orm import Session, declarative_base
from secrets_manager import get_secret
from datetime import datetime
import base64
import boto3
# from __future__ import print_function
import os
import json
import boto3
import base64
import zlib
import aws_encryption_sdk
from aws_encryption_sdk import CommitmentPolicy
from aws_encryption_sdk.internal.crypto import WrappingKey
from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
from aws_encryption_sdk.identifiers import WrappingAlgorithm, EncryptionKeyType
import re
import uuid
from df_database_models.models import Agency, Customer, Agency_Contact, Policy, LineItem, Invoice, Payment, Transaction, broker_portal_error_log
from df_database_models.db import get_db, engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION_NAME = os.environ['AWS_REGION']
RDS_RESOURCE_ID = os.environ['RDS_RESOURCE_ID']

enc_client = aws_encryption_sdk.EncryptionSDKClient(commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_ALLOW_DECRYPT)
kms = boto3.client('kms', region_name=REGION_NAME)

class MyRawMasterKeyProvider(RawMasterKeyProvider):
    provider_id = "BC"
    def __new__(cls, *args, **kwargs):
        obj = super(RawMasterKeyProvider, cls).__new__(cls)
        return obj
    def __init__(self, plain_key):
        RawMasterKeyProvider.__init__(self)
        self.wrapping_key = WrappingKey(wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                                        wrapping_key=plain_key, wrapping_key_type=EncryptionKeyType.SYMMETRIC)
    def _get_raw_key(self, key_id):
        return self.wrapping_key

def decrypt_payload(payload, data_key):
    my_key_provider = MyRawMasterKeyProvider(data_key)
    my_key_provider.add_master_key("DataKey")
    #Decrypt the records using the master key.
    decrypted_plaintext, header = enc_client.decrypt(
        source=payload,
        materials_manager=aws_encryption_sdk.materials_managers.default.DefaultCryptoMaterialsManager(master_key_provider=my_key_provider))
    return decrypted_plaintext

# Function to decompress payload
def decrypt_decompress(payload, key):
    decrypted = decrypt_payload(payload, key)
    try:
        return zlib.decompress(decrypted, zlib.MAX_WBITS + 16)
    except Exception as e:
        print("An exception occurred:", e)

def handle_das(record=None):
    data = base64.b64decode(record['kinesis']['data'])
    record_data = json.loads(data)

    # Decode and decrypt the payload
    payload_decoded = base64.b64decode(record_data['databaseActivityEvents'])
    data_key_decoded = base64.b64decode(record_data['key'])

    if 'db' in RDS_RESOURCE_ID:
        EncryptionContext={'aws:rds:db-id': RDS_RESOURCE_ID}
    else:
        EncryptionContext={'aws:rds:dbc-id': RDS_RESOURCE_ID}

    data_key_decrypt_result = kms.decrypt(CiphertextBlob=data_key_decoded,EncryptionContext=EncryptionContext)

    # if decrypt_decompress(payload_decoded, data_key_decrypt_result['Plaintext']) is None:
    #     continue

    plaintext = decrypt_decompress(payload_decoded, data_key_decrypt_result['Plaintext']).decode('utf8')

    activities = json.loads(plaintext)
    for activity in activities.get('databaseActivityEventList', []):
        print(activity)

        sql_query = activity.get('commandText', '')
        print(sql_query)
        
        if sql_query:
            if 'INSERT INTO' in sql_query.upper():
                table_name = get_insert_query_table_name(activity, sql_query)

                print(f'Table Name for DAS Source: {table_name}')

                if table_name == 'invoice_das_src':
                    invoice_dict = get_insert_query_table_dict(sql_query)

                    invoice = Invoice.from_dict(cls=Invoice, d=invoice_dict)
                    invoice.df_invoice_id = uuid.uuid4()

                    ## TODO Add logic for getting relationship attributes
                    invoice.source_system_id = 1
                    invoice.invoice_status_id = 1
                    invoice.transaction_id = "123def678t0"

                    print(f'Invoice Created from src payload: {Invoice}')

                    ## TODO Add logic to send data to SQS

                    ## TODO DB related operations to move to SQS
                    insert_into_model(entity=invoice)
                elif table_name == 'policy_das_src':
                    policy_dict = get_insert_query_table_dict(sql_query)

                    policy = Policy.from_dict(cls=Policy, d=policy_dict)
                    policy.df_policy_id = uuid.uuid4()

                    ## TODO Add logic for getting relationship attributes
                    policy.df_agency_id = '123abc678a0'
                    policy.df_agency_contact_id = '123abc678ac0'
                    policy.df_customer_id = '123abc678c0'
                    policy.policy_status_id = 3
                    policy.source_system_id = 3

                    print(f'Policy Created from src payload: {Policy}')

                    ## TODO Add logic to send data to SQS

                    ## TODO DB related operations to move to SQS
                    insert_into_model(entity=policy)

def insert_into_model(entity=None):
    session = next(get_db())
    try:
        # print(f'Adding invoice [Transaction ID: {invoice.transaction_id}] with id: {invoice.df_invoice_id}')
        session.add(entity)
        session.commit()
        # print(f'Added invoice [Transaction ID: {invoice.transaction_id}] with id: {invoice.df_invoice_id}')

    except Exception:
        # print(f'Error Adding invoice [Transaction ID: {invoice.transaction_id}] with id: {invoice.df_invoice_id}')
        session.rollback()
        raise

def get_insert_query_table_dict(sql_query):
    table_columns = [x.strip() for x in re.findall(r"INSERT\s+INTO\s+\w+.\w+\s+\(([^)]+)\)", sql_query)[0].split(',')]
                    
    print(f'Table Columns for DAS Source: {table_columns}')

    table_values = table_values = [x.strip().replace("'", '') for x in re.findall(r"VALUES\s+\(([^)]+)\)", sql_query)[0].split(',')]

    print(f'Table Values for DAS Source: {table_values}')

    invoice_dict = {}
    for i, col in enumerate(table_columns):
        invoice_dict[col] = table_values[i]
    return invoice_dict

def get_insert_query_table_name(activity, sql_query):
    table_name_l = re.findall(r"INSERT\s+INTO\s+(\w+)", sql_query)

    table_name = table_name_l[0] if len(table_name_l) > 0 else None

    db_name = activity.get('databaseName', 'datamart_workspace')

    if table_name == db_name:
        table_name = re.findall(r"INSERT\s+INTO\s+(\w+).(\w+)", sql_query)[0][1]
    return table_name


def handle_sqs(record=None):
    ##TODO
    pass

def handle(event, context):
    start_time = time.time()
    
    # print(engine)
    # print(session)
    for record in event['Records']:
        print(record)

        event_source = record.get('eventSource', '')
        if event_source == 'aws:kinesis':
            handle_das(record=record)
        elif event_source == 'aws:sqs':
            handle_sqs(record=record)
        else:
            print(f"Unidentified eventSource: {event_source}")

    end_time = time.time()

    return {
        "execution_time_sec": end_time - start_time 
    }

# if __name__ == '__main__':
#     handle({"Records": [{"body": {"limit": 10000, "offset": 0, "notebook_name": "a360v2_bcrm_agency_match_algorithms.ipynb", "notebook_output_path": "C:\\Users\\A0813048\\Downloads\\Agency 360 related files", "batch_id": "5234b860-b9b5-47c1-8988-10ce8d988036"}}]}, None)
