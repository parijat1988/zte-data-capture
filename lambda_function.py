from io import BytesIO
import os
import re
import pandas as pd
import boto3
import logging

# import pymysql
from sqlalchemy import create_engine
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class GlobalVariables:
    database_name = os.getenv('database_name')
    database_username = os.getenv('database_username')
    database_password = os.getenv('database_password')
    database_endpoint = os.getenv('database_endpoint')
    database_port = 3306
    s3_client = boto3.client('s3')
    database_uri = f"mysql+pymysql://{database_username}:{database_password}@{database_endpoint}:{database_port}/{database_name}"
    table_name = 'zte_accounts_master'


def load_df_from_s3(bucket_name, key):
    """
    Read a CSV from a S3 bucket & load into pandas dataframe
    """
    s3 = GlobalVariables.s3_client
    logger.info("Starting S3 object retrieval process...")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        logger.info("Object retrieved from S3 bucket successfully")
        excel_data = response['Body'].read()
        df = pd.read_excel(BytesIO(excel_data), header=None)
        print("Read from excel dataframe")
    except ClientError as e:
        logger.error(f"S3 object cannot be retrieved: {e}")

    return df


def data_cleaner(df):
    columns = ['month', 'rent_revenue', 'cam_revenue', 'insurance_revenue', 'property_tax_revenue',
               'landscaping_contract', 'electricity_expense', 'gas_expense',
               'water_drainage_expense', 'water_sewer_expense', 'property_insurance_expense',
               'property_tax_expense', 'property_tax_consultant', 'management_fee_expense',
               'accounting_service_expense', 'legal_services', 'note_1_interest']
    pd.set_option('display.max_columns', None)
    df_to_load = pd.DataFrame(columns=columns)
    rows_df = []
    cleansed_df = df.iloc[4:, 2:-1].reset_index(drop=True)
    sql_insert_statements = []
    for column, value in cleansed_df.items():
        values = []
        for row, value in value.items():
            if pd.notna(value):
                value_str = str(value) if not isinstance(value, str) else f"'{value}'"
                if row in [0, 7, 11, 12, 13, 25, 29, 30, 31, 32, 36, 40, 41, 45, 49, 50, 54]:
                    values.append(str(value_str))
        rows_df.append(values)
    df = pd.DataFrame(rows_df, columns=columns)
    return df


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print("Bucket name: " + str(bucket))
    logger.info(f"S3 bucket is obtained from the event: {bucket}")
    logger.info(f"Object key is obtained from the event: {key}")

    df = load_df_from_s3(bucket_name=bucket, key=key)
    df_final = data_cleaner(df)
    print(df_final)
    # upload_dataframe_into_rds(df_final)
