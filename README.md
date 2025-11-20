Serverless Financial ETL Pipeline

Project Overview

This project demonstrates a serverless data engineering pipeline built on AWS. It automates the extraction of financial market data, performs transformation and cleaning using Pandas, and loads the processed data into an S3 Data Lake.

Architecture

Extract: AWS Lambda triggers on a schedule (EventBridge) to fetch live stock data from the Yahoo Finance API.

Transform: Pandas is used to clean missing values, parse timestamps, and calculate technical indicators (50-day Moving Average).

Load: Processed data is converted to CSV and uploaded to an S3 bucket partitioned by date.

Tech Stack

Python 3.9

AWS Lambda (Compute)

Amazon S3 (Storage)

Pandas (Data Processing)

Boto3 (AWS SDK)

Setup & Deployment

Install dependencies:

pip install -r requirements.txt -t .


Zip the directory and upload to AWS Lambda.

Set environment variable BUCKET_NAME in Lambda configuration.

Attach IAM role with s3:PutObject permissions.

Sample Output

The pipeline generates a daily CSV in S3:
s3://my-financial-data-lake/processed/AAPL/2025-11-20_data.csv
