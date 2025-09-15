import os
import json
import time
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
# from df_database_models.db_conn import get_rds_db_session
from df_database_models.secrets_manager import get_secret
from df_database_models.db_utils import generate_uuid
from df_database_models.models import (
    Source_System, Customer, Submission, Quote, Product,
    Submission_Status, Submission_Type, Quote_Status, Quote_Type, Agency
)
# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# os.environ['RDS_SECRETS_MANAGER_ID'] = "arn:aws:secretsmanager:us-east-1:968921834094:secret:affinity-ods2-ar4262-dv001-rds-master-credentials-169S6S"
os.environ["AWS_REGION"] = "us-east-1"
os.environ['RDS_HOST'] = "affinity-ods2-develop.cluster-cj3qp6qspcpk.us-east-1.rds.amazonaws.com"
os.environ['RDS_DB_NAME'] = "pnc_warehouse"
# db_secret = json.loads(get_secret(
#     secret_name=os.environ["RDS_SECRETS_MANAGER_ID"],
#     region_name=os.environ["AWS_REGION"]
# ))

def get_rds_db_session(rds_host_nm, rds_db_nm):
    # rds_db_secret = json.loads(get_secret(
    #      secret_name=rds_secret_name, region_name=rds_region_name))
    rds_db_secret = {"password":"tVoCRqxYkVKnJcfD","username":"root"}
    rds_host = rds_host_nm
    rds_db_name = rds_db_nm
    df_rds_conn_str = f"mysql+pymysql://{rds_db_secret['username']}:{rds_db_secret['password']}@{rds_host}/{rds_db_name}"
    engine = create_engine(df_rds_conn_str)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()
def consume_unqork(session, config=None):
    try:
        config_dict = config if isinstance(config, dict) else json.loads(str(config))
        # 1. Source System
        source_system = session.query(Source_System).filter_by(source_system="Unqork").first()
        if not source_system:
            source_system = Source_System(source_system="Unqork")
            session.add(source_system)
            session.commit()
        # 2. Product
        product = session.query(Product).filter_by(
            program_name=config_dict.get("program_name"),
            product_name=config_dict.get("product_name")
        ).first()
        if not product:
            product = Product(
                product_id = generate_uuid(
                    str(config_dict.get('program_name')) + str(config_dict.get("product_name")), source_system.df_source_system_id
                ),
                program_name=config_dict.get("program_name"),
                product_name=config_dict.get("product_name")
            )
            session.add(product)
            session.commit()
        # 3. Submission Status
        submission_status = session.query(Submission_Status).filter_by(
            submission_status=config_dict.get("submission_status")
        ).first()
        if not submission_status:
            submission_status = Submission_Status(
                submission_status=config_dict.get("submission_status")
            )
            session.add(submission_status)
            session.commit()
        # 4. Submission Type
        submission_type = session.query(Submission_Type).filter_by(
            submission_type=config_dict.get("submission_type")
        ).first()
        if not submission_type:
            submission_type = Submission_Type(
                submission_type=config_dict.get("submission_type")
            )
            session.add(submission_type)
            session.commit()
        # 5. Customer
        customer = session.query(Customer).filter_by(
            source_customer_id=config_dict["source_customer_id"]
        ).first()
        if not customer:
            customer = Customer(
                df_customer_id = generate_uuid(
                    str(config_dict["source_customer_id"]),  source_system.df_source_system_id
                ),
                source_customer_id=config_dict["source_customer_id"],
                df_source_system_id=source_system.df_source_system_id
            )
            session.add(customer)
            session.commit()
        # 6. Submission
        submission = session.query(Submission).filter_by(
            source_submission_id=config_dict["submission_id"]
        ).first()
        if not submission:
            submission = Submission(
                df_submission_id = generate_uuid(
                    str(config_dict["submission_id"]),
                    source_system.df_source_system_id
                ),
                source_submission_id=config_dict["submission_id"],
                df_customer_id=customer.df_customer_id,
                product_id=product.product_id,
                df_source_system_id=source_system.df_source_system_id,
                df_submission_status_id=submission_status.df_submission_status_id,
                df_submission_type_id=submission_type.df_submission_type_id,
                df_agency_id = agency.df_agency_id,
                effective_date=config_dict.get("effective_date"),
                expiration_date=config_dict.get("expiration_date")
            )
            session.add(submission)
            session.commit()
        else:
            submission.effective_date = config_dict.get("effective_date")
            submission.expiration_date = config_dict.get("expiration_date")
            session.commit()

        # Agency
        agency = session.query(Agency).filter_by(
            source_agency_id=config_dict["source_agency_id"]
        ).first()
        if not agency:
            agency = Agency(
                df_agency
                _id = generate_uuid(
                    str(config_dict["source_agency_id"]),  source_system.df_source_system_id
                ),
                source_agency_id=config_dict["source_agency_id"],
                df_source_system_id=source_system.df_source_system_id
            )
            session.add(agency)
            session.commit()

        # 7. Quotes
        for proposal in config_dict.get("proposal_detail", []):
            # Quote Status
            quote_status = session.query(Quote_Status).filter_by(
                quote_status=proposal.get("proposal_status")
            ).first()
            if not quote_status:
                quote_status = Quote_Status(
                    quote_status=proposal.get("proposal_status")
                )
                session.add(quote_status)
                session.commit()
            # Quote Type
            quote_type = session.query(Quote_Type).filter_by(
                quote_type=proposal.get("proposal_type")
            ).first()
            if not quote_type:
                quote_type = Quote_Type(
                    quote_type=proposal.get("proposal_type")
                )
                session.add(quote_type)
                session.commit()
            quote = session.query(Quote).filter_by(
                source_quote_id=proposal["proposal_id"]
            ).first()
            if not quote:
                quote = Quote(
                    df_quote_id = generate_uuid(
                        str(proposal["proposal_id"]),
                        source_system.df_source_system_id
                    ),
                    source_quote_id=proposal["proposal_id"],
                    df_submission_id=submission.df_submission_id,
                    df_source_system_id=source_system.df_source_system_id,
                    df_quote_status_id=quote_status.df_quote_status_id,
                    df_quote_type_id=quote_type.df_quote_type_id,
                    effective_date=proposal.get("proposal_effective_date"),
                    expiration_date=proposal.get("proposal_expiration_date")
                )
                session.add(quote)
                session.commit()
            else:
                quote.effective_date = proposal.get("proposal_effective_date")
                quote.expiration_date = proposal.get("proposal_expiration_date")
                session.commit()
        return {"status": "success"}
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"DB Error: {e}")
        raise e
# os.environ["RDS_SECRETS_MANAGER_ID"],
#         os.environ["AWS_REGION"],
# Lambda-style handler
def handle(event, context):
    start_time = time.time()
    session = get_rds_db_session(
        os.environ["RDS_HOST"],
        os.environ["RDS_DB_NAME"]
    )
    for record in event["Records"]:
        payload = record["body"]
        s = consume_unqork(session, config=payload)
        print(s)
    session.close()
    end_time = time.time()
    return {"execution_time_sec": end_time - start_time}

if __name__ == '__main__':
    handle({"Records": [{'body': {"source_customer_id": "CUST12345","customer_name": "John Doe","submission_id": "SUBM11111","program_name": "Homeowners","product_name": "Basic Coverage","submission_status": "Open","submission_type": "New Business","effective_date": "2025-09-11T00:00:00","expiration_date": "2025-09-14T23:59:59", "source_agency_id" : "CUS1100","proposal_detail": [{"proposal_id": "QUOTE9991","proposal_status": "In Progress","proposal_effective_date": "2025-01-01T00:00:00","proposal_expiration_date": "2025-12-31T23:59:59"},{"proposal_id": "QUOTE9992","proposal_status": "Issued","proposal_effective_date": "2025-01-15T00:00:00","proposal_expiration_date": "2026-01-14T23:59:59"}]}}]}, None) 