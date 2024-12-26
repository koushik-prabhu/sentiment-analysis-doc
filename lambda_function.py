import json
import boto3
import pymysql
import csv
import urllib.parse
import os

# Fetch environment variables
DB_HOST = '##'
DB_USER = '##'
DB_PASSWORD = '##'
DB_NAME = '##'

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Retrieve bucket name and file name from event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_name = event['Records'][0]['s3']['object']['key']
        file_name = urllib.parse.unquote(file_name)
        file_name = file_name.replace('+', ' ')
        simple_file_name = file_name.split('/')[-1]

        print('Bucketname: ', bucket_name)
        print("Filename: ", file_name)
        
        # Download file from S3
        try:
            file_path = f'/tmp/{simple_file_name}'
            s3_client.download_file(bucket_name, file_name, file_path)
        except Exception as e:
            print('Failed to downlaod', e)
        
        # Read CSV file
        with open(file_path, 'r') as file:
            print('File opend to read...')
            try:
                csv_data = csv.DictReader(file)
                header = next(csv_data)  # Skip the header row
                try:
                    # Connect to RDS MySQL
                    connection = pymysql.connect(
                        host=DB_HOST,
                        user=DB_USER,
                        password=DB_PASSWORD,
                        database=DB_NAME
                    )
                    cursor = connection.cursor()
                except Exception as e:
                    print('RDS error',e)
                
                insert_query = "INSERT IGNORE INTO user_reviews (phone,title,review,source,score) VALUES (%s,%s,%s,%s,%s)"
                # Insert rows from the CSV file
                for row in csv_data:
                    cursor.execute(insert_query, (row['phone'],row['title'],row['review'],row['data source'],int(row['sentiment'])))
                
                print('comminting about to start')
                # Commit and close connection
                connection.commit()
                print(f"Successfully inserted rows")
                # Ensure transaction was committed
                connection.commit()
                cursor.close()
                connection.close()
            except Exception as e:
                print('Failed, ',e)

        return {
            'statusCode': 200,
            'body': json.dumps(f"File '{file_name}' processed successfully.")
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

    print('Completed')
