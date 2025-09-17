import json
import pandas as pd
import boto3
import logging
from io import StringIO
import os
from typing import Dict, Any, List
from datetime import datetime
 
# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
# Initialize S3 client
s3_client = boto3.client('s3')
 
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Combined AWS Lambda handler that:
    1. Processes CSV sales data from input folder OR uses existing processed file
    2. Merges the output with target data
    """
    try:
        # Use your specific S3 bucket
        bucket_name = event.get('bucket_name', 'grg-test-01')
        output_bucket = event.get('output_bucket', 'grg-test-01')
       
        # Target file configuration - updated to use August targets since that's what you have
        target_file = event.get('target_file', 'input/August final targets.csv')
       
        # Check if user wants to use existing processed file or process new CSV files
        existing_sales_file = event.get('existing_sales_file', 'output/combined/Adani_east_n1_n2_south_central_west_final_2025-09-11_10-00-20.csv')
        skip_phase1 = event.get('skip_phase1', True)  # Default to use existing file
       
        logger.info(f"🚀 Starting combined processing for bucket: {bucket_name}")
        logger.info(f"🎯 Target file: {target_file}")
        logger.info(f"📊 Skip Phase 1 (use existing): {skip_phase1}")
       
        if skip_phase1 and existing_sales_file:
            # Use existing processed sales file
            logger.info(f"📂 Using existing sales file: {existing_sales_file}")
           
            if not check_file_exists(bucket_name, existing_sales_file):
                return {
                    'statusCode': 404,
                    'body': {
                        'error': f'Existing sales file not found: {existing_sales_file}',
                        'bucket': bucket_name,
                        'available_output_files': list_files_in_folder(bucket_name, 'output'),
                        'suggestions': [
                            'Run Phase 1 first to create the combined sales file',
                            'Check if the file path is correct',
                            'Set skip_phase1 to false to process CSV files first'
                        ]
                    }
                }
           
            # Load existing sales file
            combined_sales_df = load_csv_from_s3(bucket_name, existing_sales_file)
           
            # Create mock Phase 1 metrics for existing file
            phase1_metrics = {
                'individual_file_counts': {'existing_file': len(combined_sales_df)},
                'combined_rows_before_processing': len(combined_sales_df),
                'final_processed_rows': len(combined_sales_df),
                'total_sales': combined_sales_df['Sales'].sum() if 'Sales' in combined_sales_df.columns else 0,
                'unique_customers': combined_sales_df['Customer Code'].nunique() if 'Customer Code' in combined_sales_df.columns else 0,
                'zone_counts': combined_sales_df['Zone'].value_counts().to_dict() if 'Zone' in combined_sales_df.columns else {},
                'files_processed': [existing_sales_file],
                'missing_files': [],
                'master_file_used': True,
                'phase1_output_file': existing_sales_file,
                'customer_sales_file': existing_sales_file.replace('.csv', '_customer_sales.csv'),
                'processing_time': datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'),
                'phase1_skipped': True
            }
           
            logger.info(f"✅ Loaded existing sales file: {len(combined_sales_df):,} rows")
        else:
            # PHASE 1: Process CSV files and create combined sales data
            logger.info("🔄 PHASE 1: Processing individual CSV files...")
            combined_sales_df, phase1_metrics = process_csv_files(bucket_name, output_bucket)
           
            if combined_sales_df is None:
                return phase1_metrics  # Return error response from Phase 1
       
        # PHASE 2: Merge with target data
        logger.info("🔄 PHASE 2: Merging with target data...")
        final_result = merge_with_targets(combined_sales_df, bucket_name, output_bucket, target_file, phase1_metrics)
       
        return final_result
       
    except Exception as e:
        logger.error(f"💥 Error in combined lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Combined processing failed',
                'bucket': bucket_name,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
 
def process_csv_files(bucket_name: str, output_bucket: str) -> tuple:
    """
    PHASE 1: Process individual CSV files and combine them
    """
    try:
        # CSV file mappings based on your actual file names
        csv_files = {
            'input/East August.csv': 'East',
            'input/North 1 Aug.csv': 'North 1',
            'input/North 2 August.csv': 'North 2',
            'input/South new.csv': 'South',
            'input/West August.csv': 'West',
            'input/Central august.csv': 'Central'
        }
       
        master_file = 'input/Material Master August.xlsx'
       
        # Check which files exist
        input_files = list_files_in_folder(bucket_name, 'input')
        logger.info(f"📁 Files in 'input' folder: {input_files}")
       
        existing_files = {}
        missing_files = []
       
        for file_path, zone in csv_files.items():
            if check_file_exists(bucket_name, file_path):
                existing_files[file_path] = zone
                logger.info(f"✅ Found: {file_path}")
            else:
                missing_files.append(file_path)
                logger.warning(f"❌ Missing: {file_path}")
       
        if not existing_files:
            return None, {
                'statusCode': 404,
                'body': {
                    'error': 'No CSV files found in input folder',
                    'phase': 'Phase 1 - CSV Processing',
                    'bucket': bucket_name,
                    'expected_files': list(csv_files.keys()),
                    'missing_files': missing_files,
                    'files_in_input_folder': input_files
                }
            }
       
        # Process existing files
        combined_dataframes = []
        zone_counts = {}
        individual_file_counts = {}
       
        for file_path, zone in existing_files.items():
            try:
                df = load_csv_from_s3(bucket_name, file_path)
                df['Zone'] = zone
                combined_dataframes.append(df)
                zone_counts[zone] = len(df)
                individual_file_counts[file_path.split('/')[-1]] = len(df)
                logger.info(f"✅ Loaded {file_path} with {len(df)} rows for zone {zone}")
            except Exception as e:
                logger.error(f"❌ Error loading {file_path}: {str(e)}")
                zone_counts[zone] = 0
                continue
       
        if not combined_dataframes:
            return None, {
                'statusCode': 500,
                'body': {
                    'error': 'Files found but could not be loaded',
                    'phase': 'Phase 1 - CSV Processing',
                    'bucket': bucket_name,
                    'existing_files': list(existing_files.keys())
                }
            }
       
        # Combine all DataFrames
        df_combined = pd.concat(combined_dataframes, ignore_index=True)
        combined_rows_before_processing = len(df_combined)
        logger.info(f"✅ Combined data has {combined_rows_before_processing} total rows")
       
        # Load and merge with Material Master if exists
        master_exists = check_file_exists(bucket_name, master_file)
        if master_exists:
            try:
                df_master = load_excel_from_s3(bucket_name, master_file)
                df_combined = merge_with_master(df_combined, df_master)
                logger.info("✅ Successfully merged with Material Master data")
            except Exception as e:
                logger.warning(f"⚠️ Could not merge with Material Master: {str(e)}")
       
        # Calculate sales
        df_combined = calculate_sales(df_combined)
       
        # Aggregate sales by customer
        sales_by_customer = aggregate_sales_by_customer(df_combined)
       
        # Save Phase 1 results to S3
        current_time = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        phase1_output_key = f"output/combined/Adani_east_n1_n2_south_central_west_final_{current_time}.csv"
        customer_sales_key = f"output/customer-sales/sales_by_customer_{current_time}.csv"
       
        save_dataframe_to_s3(df_combined, output_bucket, phase1_output_key)
        save_dataframe_to_s3(sales_by_customer, output_bucket, customer_sales_key)
       
        # Calculate Phase 1 metrics
        total_sales = df_combined['Sales'].sum()
        unique_customers = df_combined['Customer Code'].nunique() if 'Customer Code' in df_combined.columns else 0
        final_processed_rows = len(df_combined)
       
        phase1_metrics = {
            'individual_file_counts': individual_file_counts,
            'combined_rows_before_processing': combined_rows_before_processing,
            'final_processed_rows': final_processed_rows,
            'total_sales': total_sales,
            'unique_customers': unique_customers,
            'zone_counts': zone_counts,
            'files_processed': list(existing_files.keys()),
            'missing_files': missing_files,
            'master_file_used': master_exists,
            'phase1_output_file': phase1_output_key,
            'customer_sales_file': customer_sales_key,
            'processing_time': current_time,
            'phase1_skipped': False
        }
       
        logger.info(f"✅ Phase 1 completed: {final_processed_rows:,} rows, {unique_customers:,} customers")
        return df_combined, phase1_metrics
       
    except Exception as e:
        logger.error(f"💥 Error in Phase 1: {str(e)}")
        return None, {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'phase': 'Phase 1 - CSV Processing',
                'bucket': bucket_name
            }
        }
 
def merge_with_targets(sales_df: pd.DataFrame, bucket_name: str, output_bucket: str, target_file: str, phase1_metrics: dict) -> Dict[str, Any]:
    """
    PHASE 2: Merge the combined sales data with target data
    """
    try:
        current_time = phase1_metrics['processing_time']
       
        # Check if target file exists
        if not check_file_exists(bucket_name, target_file):
            return {
                'statusCode': 404,
                'body': {
                    'phase1_completed': True,
                    'phase1_metrics': phase1_metrics,
                    'phase2_error': f'Target file not found: {target_file}',
                    'message': 'Phase 1 completed successfully, Phase 2 failed',
                    'available_files': list_files_in_folder(bucket_name, 'input'),
                    'suggestion': 'Use "input/August final targets.csv" which is available in your bucket'
                }
            }
       
        # Load target data
        logger.info(f"📥 Loading target data from: {target_file}")
        target_df = load_csv_from_s3(bucket_name, target_file)
       
        target_rows = len(target_df)
        sales_rows = len(sales_df)
       
        logger.info(f"📊 Sales data: {sales_rows:,} rows")
        logger.info(f"🎯 Target data: {target_rows:,} rows")
       
        # Clean Customer Code columns (same as notebook)
        logger.info("🧹 Cleaning Customer Code columns...")
        sales_df['Customer Code'] = sales_df['Customer Code'].astype(str).str.strip()
        target_df['Customer Code'] = target_df['Customer Code'].astype(str).str.strip()
       
        # Log sample Customer Codes for debugging
        logger.info(f"📋 Sample Sales Customer Codes: {sales_df['Customer Code'].head().tolist()}")
        logger.info(f"📋 Sample Target Customer Codes: {target_df['Customer Code'].head().tolist()}")
       
        # Merge sales and target data on Customer Code (inner join)
        logger.info("🔗 Merging sales and target data...")
        matched_sales_df = pd.merge(sales_df, target_df, on='Customer Code', how='inner')
       
        matched_rows = len(matched_sales_df)
        unique_customers_matched = matched_sales_df['Customer Code'].nunique()
       
        logger.info(f"✅ Merge completed: {matched_rows:,} rows matched")
        logger.info(f"👥 Unique customers matched: {unique_customers_matched:,}")
       
        # Create Sales column for target analysis (same as notebook)
        if 'Quantity in UOM1(KG)' in matched_sales_df.columns:
            matched_sales_df['Sales_Target_Analysis'] = matched_sales_df['Quantity in UOM1(KG)']
        else:
            matched_sales_df['Sales_Target_Analysis'] = matched_sales_df['Sales']
       
        total_sales_matched = matched_sales_df['Sales_Target_Analysis'].sum()
       
        # Save Phase 2 result (same filename as notebook)
        phase2_output_filename = f"Adani_1-14th_july_matched_{current_time}.csv"
        phase2_output_key = f"output/merged/{phase2_output_filename}"
       
        save_dataframe_to_s3(matched_sales_df, output_bucket, phase2_output_key)
       
        # Calculate target analysis
        target_analysis = calculate_target_analysis(matched_sales_df)
       
        # Get file sizes
        phase2_file_size = get_file_size_mb(output_bucket, phase2_output_key)
       
        # Calculate match percentages
        match_percentage = (matched_rows / sales_rows) * 100 if sales_rows > 0 else 0
        customer_match_rate = (unique_customers_matched / phase1_metrics['unique_customers']) * 100 if phase1_metrics['unique_customers'] > 0 else 0
       
        # Prepare comprehensive response
        result = {
            'statusCode': 200,
            'body': {
                'message': '🎉 Combined processing completed successfully!',
                'bucket': bucket_name,
                'processed_at': current_time,
               
                # 🔄 PHASE SUMMARY
                'phase_summary': {
                    'phase1_status': 'skipped' if phase1_metrics.get('phase1_skipped') else 'completed',
                    'phase2_status': 'completed',
                    'processing_flow': 'Existing Sales File → Target Merge' if phase1_metrics.get('phase1_skipped') else 'CSV Files → Combined Sales → Target Merge'
                },
               
                # 📊 PHASE 1 RESULTS
                'phase1_results': {
                    'source': 'existing_file' if phase1_metrics.get('phase1_skipped') else 'csv_processing',
                    'sales_file_used': phase1_metrics['phase1_output_file'],
                    'total_sales_rows': phase1_metrics['final_processed_rows'],
                    'total_sales': float(phase1_metrics['total_sales']),
                    'unique_customers': phase1_metrics['unique_customers'],
                    'zones_in_data': phase1_metrics['zone_counts']
                },
               
                # 🎯 PHASE 2 RESULTS (Matching Notebook Results)
                'phase2_results': {
                    'sales_df_rows': sales_rows,           # From Phase 1 output or existing file
                    'target_df_rows': target_rows,         # August final targets.csv
                    'matched_sales_df_rows': matched_rows, # After inner join
                    'unique_customers_matched': unique_customers_matched,
                    'total_sales_matched': float(total_sales_matched),
                    'match_percentage': round(match_percentage, 2),
                    'customer_match_rate': round(customer_match_rate, 2)
                },
               
                # 💰 BUSINESS ANALYSIS
                'business_analysis': {
                    'total_sales_all_data': float(phase1_metrics['total_sales']),
                    'total_sales_with_targets': float(total_sales_matched),
                    'sales_coverage_percentage': round((total_sales_matched / phase1_metrics['total_sales']) * 100, 2) if phase1_metrics['total_sales'] > 0 else 0,
                    'average_sales_per_customer': round(float(total_sales_matched) / unique_customers_matched, 3) if unique_customers_matched > 0 else 0,
                    'target_analysis': target_analysis
                },
               
                # 📁 OUTPUT FILES
                'output_files': {
                    'sales_source_file': {
                        'path': f"s3://{bucket_name}/{phase1_metrics['phase1_output_file']}",
                        'filename': phase1_metrics['phase1_output_file'].split('/')[-1],
                        'rows': phase1_metrics['final_processed_rows'],
                        'description': 'Source sales data used for target matching'
                    },
                    'final_matched_data': {
                        'path': f"s3://{output_bucket}/{phase2_output_key}",
                        'filename': phase2_output_filename,
                        'rows': matched_rows,
                        'columns': len(matched_sales_df.columns),
                        'size_mb': phase2_file_size,
                        'description': 'Final output: Sales data matched with targets (equivalent to notebook output)'
                    }
                },
               
                # 🎯 NOTEBOOK COMPARISON
                'notebook_comparison': {
                    'target_file_used': target_file,
                    'expected_pattern': 'sales_df + target_df → matched_sales_df',
                    'actual_result': f"{sales_rows:,} + {target_rows:,} → {matched_rows:,}",
                    'notebook_equivalent_output': phase2_output_filename,
                    'notebook_command': "matched_sales_df.to_csv('Adani 1-14th july.csv', index=False)"
                },
               
                # 🎯 QUICK SUMMARY
                'quick_summary': {
                    'message': f"✅ Used existing sales file ({sales_rows:,} rows) → matched with targets ({matched_rows:,} rows)",
                    'customers_with_targets': f"{unique_customers_matched:,}",
                    'total_sales_formatted': f"{total_sales_matched:,.3f} KG",
                    'match_efficiency': f"{round(match_percentage, 1)}%",
                    'final_output': phase2_output_filename,
                    'processing_note': 'Used existing processed sales file from previous run'
                }
            }
        }
       
        # Log comprehensive results
        logger.info(f"🎉 Combined processing completed successfully!")
        logger.info(f"📊 Source: {sales_rows:,} rows from existing sales file")
        logger.info(f"🎯 Target: {target_rows:,} rows from {target_file}")
        logger.info(f"🔗 Matched: {matched_rows:,} rows")
        logger.info(f"💰 Total matched sales: {total_sales_matched:,.3f} KG")
        logger.info(f"👥 Customers with targets: {unique_customers_matched:,}")
        logger.info(f"📁 Final output: {phase2_output_filename}")
       
        return result
       
    except Exception as e:
        logger.error(f"💥 Error in Phase 2: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'phase1_completed': True,
                'phase1_metrics': phase1_metrics,
                'phase2_error': str(e),
                'message': 'Phase 1 completed successfully, Phase 2 failed',
                'bucket': bucket_name
            }
        }
 
def calculate_target_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate target vs sales analysis"""
    try:
        target_col = None
        sales_col = None
       
        # Find target column
        for col in ['Final Target (KG)', 'Target (KG)', 'Target']:
            if col in df.columns:
                target_col = col
                break
       
        # Find sales column
        for col in ['Sales_Target_Analysis', 'Sales', 'Quantity in UOM1(KG)']:
            if col in df.columns:
                sales_col = col
                break
       
        if not target_col or not sales_col:
            return {
                'error': f'Required columns not found. Available columns: {list(df.columns)}',
                'target_col_searched': ['Final Target (KG)', 'Target (KG)', 'Target'],
                'sales_col_searched': ['Sales_Target_Analysis', 'Sales', 'Quantity in UOM1(KG)']
            }
       
        # Convert to numeric
        df[target_col] = pd.to_numeric(df[target_col], errors='coerce').fillna(0)
        df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
       
        # Calculate metrics
        total_target = df[target_col].sum()
        total_sales = df[sales_col].sum()
        achievement_percentage = (total_sales / total_target * 100) if total_target > 0 else 0
       
        # Customer-wise analysis
        customer_analysis = df.groupby('Customer Code').agg({
            target_col: 'first',
            sales_col: 'sum'
        }).reset_index()
       
        customer_analysis['Achievement %'] = (customer_analysis[sales_col] / customer_analysis[target_col] * 100).fillna(0)
       
        customers_above_target = len(customer_analysis[customer_analysis['Achievement %'] >= 100])
        customers_below_target = len(customer_analysis[customer_analysis['Achievement %'] < 100])
       
        return {
            'target_column_used': target_col,
            'sales_column_used': sales_col,
            'total_target_kg': float(total_target),
            'total_sales_kg': float(total_sales),
            'overall_achievement_percentage': round(achievement_percentage, 2),
            'customers_above_target': customers_above_target,
            'customers_below_target': customers_below_target,
            'target_gap_kg': float(total_target - total_sales),
            'customers_analyzed': len(customer_analysis)
        }
       
    except Exception as e:
        logger.error(f"Error in target analysis: {str(e)}")
        return {'error': str(e)}
 
def list_files_in_folder(bucket_name: str, folder: str) -> list:
    """List all files in a specific S3 folder"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{folder}/"
        )
        files = []
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
        return files
    except Exception as e:
        logger.error(f"Error listing files in {folder}: {str(e)}")
        return []
 
# All the helper functions from the previous version remain the same
def check_file_exists(bucket_name: str, key: str) -> bool:
    """Check if a file exists in S3 bucket"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except Exception:
        return False
 
def load_csv_from_s3(bucket_name: str, key: str) -> pd.DataFrame:
    """Load CSV file from S3 bucket"""
    try:
        logger.info(f"📥 Loading {key} from bucket {bucket_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('ISO-8859-1')
        df = pd.read_csv(StringIO(csv_content), encoding='ISO-8859-1', low_memory=False)
        logger.info(f"✅ Successfully loaded {key} with {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"❌ Error loading {key}: {str(e)}")
        raise
 
def load_excel_from_s3(bucket_name: str, key: str) -> pd.DataFrame:
    """Load Excel file from S3 bucket"""
    try:
        logger.info(f"📥 Loading Excel file {key} from bucket {bucket_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        excel_content = response['Body'].read()
        df = pd.read_excel(excel_content, engine='openpyxl')
        logger.info(f"✅ Successfully loaded {key} with {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"❌ Error loading Excel file {key}: {str(e)}")
        raise
 
def merge_with_master(df_combined: pd.DataFrame, df_master: pd.DataFrame) -> pd.DataFrame:
    """Merge combined data with Material Master"""
    df_combined.columns = df_combined.columns.str.strip()
    df_master.columns = df_master.columns.str.strip()
   
    logger.info(f"📋 Master file has {len(df_master)} rows and columns: {list(df_master.columns)}")
   
    if 'SKU Code' not in df_master.columns or 'SKU Code' not in df_combined.columns:
        logger.error("❌ 'SKU Code' column not found")
        return df_combined
   
    if 'Product Type (UPS)' in df_master.columns:
        df_merged = df_combined.merge(
            df_master[['SKU Code', 'Product Type (UPS)']],
            on='SKU Code',
            how='left'
        )
    else:
        df_merged = df_combined.merge(
            df_master[['SKU Code']],
            on='SKU Code',
            how='left'
        )
   
    logger.info(f"✅ Merged data shape: {df_merged.shape}")
    return df_merged
 
def calculate_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Sales column: (Quantity in UOM1(KG) - Free Quantity) with minimum value of 0"""
    try:
        required_columns = ['Quantity in UOM1(KG)', 'Free Quantity']
        missing_columns = [col for col in required_columns if col not in df.columns]
       
        if missing_columns:
            logger.error(f"❌ Missing columns for sales calculation: {missing_columns}")
            df['Sales'] = 0
            return df
       
        quantity_uom = pd.to_numeric(df['Quantity in UOM1(KG)'], errors='coerce').fillna(0)
        free_quantity = pd.to_numeric(df['Free Quantity'], errors='coerce').fillna(0)
       
        df['Sales'] = (quantity_uom - free_quantity).clip(lower=0)
       
        total_sales = df['Sales'].sum()
        logger.info(f"💰 Calculated sales. Total sales: {total_sales}")
       
        return df
       
    except Exception as e:
        logger.error(f"❌ Error calculating sales: {str(e)}")
        df['Sales'] = 0
        return df
 
def aggregate_sales_by_customer(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sum of Sales by Customer Code"""
    try:
        if 'Customer Code' not in df.columns:
            logger.error("❌ 'Customer Code' column not found")
            return pd.DataFrame(columns=['Customer Code', 'Sales'])
       
        sales_by_customer = df.groupby('Customer Code')['Sales'].sum().reset_index()
       
        additional_columns = ['Customer Name', 'Zone', 'City', 'State', 'Business Channel', 'Distributor Name']
        available_columns = [col for col in additional_columns if col in df.columns]
       
        if available_columns:
            try:
                agg_dict = {col: 'first' for col in available_columns}
                customer_info = df.groupby('Customer Code').agg(agg_dict).reset_index()
                sales_by_customer = sales_by_customer.merge(customer_info, on='Customer Code', how='left')
            except Exception as info_error:
                logger.warning(f"⚠️ Could not add customer info: {str(info_error)}")
       
        sales_by_customer = sales_by_customer.sort_values('Sales', ascending=False)
        logger.info(f"👥 Aggregated sales for {len(sales_by_customer)} customers")
        return sales_by_customer
       
    except Exception as e:
        logger.error(f"❌ Error aggregating sales by customer: {str(e)}")
        return pd.DataFrame(columns=['Customer Code', 'Sales'])
 
def save_dataframe_to_s3(df: pd.DataFrame, bucket_name: str, key: str) -> None:
    """Save DataFrame to S3 bucket as CSV"""
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
       
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv',
            ServerSideEncryption='AES256'
        )
       
        logger.info(f"📤 Successfully saved {len(df)} rows to s3://{bucket_name}/{key}")
       
    except Exception as e:
        logger.error(f"❌ Error saving to S3: {str(e)}")
        raise
 
def get_file_size_mb(bucket_name: str, key: str) -> float:
    """Get file size in MB"""
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        size_bytes = response['ContentLength']
        return round(size_bytes / (1024 * 1024), 2)
    except Exception:
        return 0.0