print("Hello")
#this is lamda script --
import json
import pandas as pd
import boto3
import logging
from io import BytesIO, StringIO
from typing import Dict, Any
from datetime import datetime, timezone
import os
from botocore.config import Config

# ----------------------------
# Configure logging
# ----------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ----------------------------
# S3 Client with timeout
# ----------------------------
s3_config = Config(connect_timeout=5, read_timeout=10)

# For local testing: use env variables
# On Lambda: just create client, it will use IAM Role
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name='ap-south-1',
    config=s3_config
)

# ----------------------------
# S3 Helper Functions
# ----------------------------
def load_csv_from_s3(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read()
    return pd.read_csv(BytesIO(data), encoding='latin-1')

def load_excel_from_s3(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read()
    return pd.read_excel(BytesIO(data))

def save_dataframe_to_s3(df, bucket_name, file_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding='latin-1')
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
    logger.info(f"✅ Saved to s3://{bucket_name}/{file_key}")

def check_file_exists(bucket_name, file_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except:
        return False

def list_files_in_folder(bucket_name, folder_prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    if 'Contents' in response:
        return [item['Key'] for item in response['Contents']]
    return []

# ----------------------------
# Data Processing Functions
# ----------------------------
def merge_with_targets(df, bucket, output_bucket, target_file, metrics):
    logger.info("Merging with targets")
    if check_file_exists(bucket, target_file):
        target_df = load_csv_from_s3(bucket, target_file)
        merged_df = df.merge(target_df, on='Customer Code', how='left')
        missing_targets = merged_df['Customer Code'].isna().sum()
        logger.info(f"Customers missing target after merge: {missing_targets}")
        return {"status": "merged", "rows": len(merged_df)}
    else:
        logger.warning(f"Target file not found: {target_file}")
        return {"status": "target_file_missing", "rows": len(df)}

def merge_with_master(df, df_master):
    logger.info("Merging with master")
    return df.merge(df_master, on='Material Code', how='left') if 'Material Code' in df_master.columns else df

def calculate_sales(df):
    logger.info("Calculating sales")
    if 'Sales' not in df.columns:
        df['Sales'] = 0
    return df

def aggregate_sales_by_customer(df):
    logger.info("Aggregating sales by customer")
    if 'Customer Code' in df.columns:
        return df.groupby('Customer Code', as_index=False)['Sales'].sum()
    return df

# ----------------------------
# S3 Folders
# ----------------------------
INPUT_FOLDER = 'Adani August 2025/Input'
OUTPUT_FOLDER = 'Adani August 2025/Output'

# ----------------------------
# Main Lambda Handler
# ----------------------------
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        bucket_name = event.get('bucket_name', 'adani-data-s3')
        output_bucket = event.get('output_bucket', 'adani-data-s3')
        target_file = event.get('target_file', f'{INPUT_FOLDER}/August final targets.csv')
        existing_sales_file = event.get('existing_sales_file', None)
        skip_phase1 = event.get('skip_phase1', True)

        # List all files in input folder
        all_files = list_files_in_folder(bucket_name, INPUT_FOLDER)
        logger.info("📂 Files found in S3 under INPUT_FOLDER:")
        for f in all_files:
            logger.info(f" - {f}")

        # Phase 1: Load/Combine CSVs
        if skip_phase1 and existing_sales_file:
            logger.info(f"📂 Using existing sales file: {existing_sales_file}")
            if not check_file_exists(bucket_name, existing_sales_file):
                return {'statusCode': 404, 'body': f'File not found: {existing_sales_file}'}
            combined_sales_df = load_csv_from_s3(bucket_name, existing_sales_file)
            phase1_metrics = {
                'phase1_skipped': True,
                'phase1_output_file': existing_sales_file,
                'unique_customers': combined_sales_df['Customer Code'].nunique()
            }
        else:
            combined_sales_df, phase1_metrics = process_csv_files(bucket_name, output_bucket)

        # Phase 2: Merge with target
        if combined_sales_df is None:
            return {'statusCode': 500, 'body': 'Phase 1 failed: No data processed'}

        final_result = merge_with_targets(combined_sales_df, bucket_name, output_bucket, target_file, phase1_metrics)
        logger.info(final_result)
        return final_result

    except Exception as e:
        logger.error(f"💥 Error: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}

# ----------------------------
# Process CSV Files
# ----------------------------
def process_csv_files(bucket_name: str, output_bucket: str) -> tuple:
    try:
        csv_files = {
            f'{INPUT_FOLDER}/East August.csv': 'East',
            f'{INPUT_FOLDER}/North 1 Aug.csv': 'North 1',
            f'{INPUT_FOLDER}/North 2 August.csv': 'North 2',
            f'{INPUT_FOLDER}/South new.csv': 'South',
            f'{INPUT_FOLDER}/Central august.csv': 'Central'
        }
        master_file = f'{INPUT_FOLDER}/Material Master August.xlsx'

        existing_files = {}
        for file_path, zone in csv_files.items():
            if check_file_exists(bucket_name, file_path):
                existing_files[file_path] = zone

        if not existing_files:
            raise Exception("No CSV files found to process!")

        combined_dataframes = []
        zone_counts = {}
        individual_file_counts = {}

        for file_path, zone in existing_files.items():
            df = load_csv_from_s3(bucket_name, file_path)
            df['Zone'] = zone
            combined_dataframes.append(df)
            zone_counts[zone] = len(df)
            individual_file_counts[file_path.split('/')[-1]] = len(df)

        df_combined = pd.concat(combined_dataframes, ignore_index=True)

        if check_file_exists(bucket_name, master_file):
            df_master = load_excel_from_s3(bucket_name, master_file)
            df_combined = merge_with_master(df_combined, df_master)

        df_combined = calculate_sales(df_combined)
        sales_by_customer = aggregate_sales_by_customer(df_combined)

        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
        phase1_output_key = f"{OUTPUT_FOLDER}/combined_sales_{current_time}.csv"
        customer_sales_key = f"{OUTPUT_FOLDER}/sales_by_customer_{current_time}.csv"

        save_dataframe_to_s3(df_combined, output_bucket, phase1_output_key)
        save_dataframe_to_s3(sales_by_customer, output_bucket, customer_sales_key)

        phase1_metrics = {
            'individual_file_counts': individual_file_counts,
            'zone_counts': zone_counts,
            'phase1_output_file': phase1_output_key,
            'customer_sales_file': customer_sales_key,
            'processing_time': current_time,
            'final_processed_rows': len(df_combined),
            'total_sales': df_combined['Sales'].sum(),
            'unique_customers': df_combined['Customer Code'].nunique(),
            'phase1_skipped': False
        }

        return df_combined, phase1_metrics

    except Exception as e:
        logger.error(f"💥 Error in Phase 1: {str(e)}")
        return None, {'error': str(e)}

# ----------------------------
# Run locally
# ----------------------------
if __name__ == "__main__":
    event = {
        "bucket_name": "adani-data-s3",
        "output_bucket": "adani-data-s3",
        "skip_phase1": False
    }
    result = lambda_handler(event, None)
    print("✅ Script finished, result:", result)
