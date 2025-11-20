import json
import os
import boto3
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

# Initialize S3 Client
s3 = boto3.client('s3')

# Configuration (Env variables are best for AWS Lambda)
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'my-financial-data-lake')
SYMBOL = 'AAPL'
API_URL = f"https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL}"

def lambda_handler(event, context):
    """
    AWS Lambda Entry Point.
    Triggers on a schedule (CloudWatch Event) to fetch, process, and store data.
    """
    try:
        print(f"Starting ETL job for {SYMBOL}...")
        
        # 1. EXTRACT: Fetch data from external API
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(API_URL, headers=headers)
        data = response.json()
        
        # Parse specific JSON structure (Yahoo Finance specific)
        timestamps = data['chart']['result'][0]['timestamp']
        quotes = data['chart']['result'][0]['indicators']['quote'][0]
        
        # 2. TRANSFORM: Clean and Process with Pandas
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(timestamps, unit='s'),
            'open': quotes['open'],
            'high': quotes['high'],
            'low': quotes['low'],
            'close': quotes['close'],
            'volume': quotes['volume']
        })
        
        # Drop missing values
        df.dropna(inplace=True)
        
        # Feature Engineering: Calculate 50-day Moving Average
        df['MA_50'] = df['close'].rolling(window=50).mean()
        
        # Data Quality Check
        if df.empty:
            raise ValueError("Dataframe is empty after processing!")
            
        print("Transformation complete. Data shape:", df.shape)
        
        # 3. LOAD: Save to S3 as CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        file_name = f"processed/{SYMBOL}/{datetime.now().strftime('%Y-%m-%d')}_data.csv"
        
        # Note: This requires AWS Credentials in environment or IAM Role attached to Lambda
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=csv_buffer.getvalue()
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully processed and uploaded {file_name}")
        }

    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

# For local testing only
if __name__ == "__main__":
    # Mock context for local run
    lambda_handler(None, None)
