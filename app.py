import os
import boto3
import requests
import json
import snowflake.connector as snowflake_connector
from dotenv import load_dotenv
from datetime import date
load_dotenv()


def get_list_of_zip_codes(conn):
	cursor = conn.cursor()
	cursor.execute("SELECT zip_code FROM location WHERE state = 'DE'")
	results = (r[0] for r in cursor.fetchall())
	conn.close()
	return results

def get_results_for_zipcodes(zipcodes, api_key):
	url = 'https://crime-data-by-zipcode-api.p.rapidapi.com/crime_data'
	headers = {
		'X-RapidAPI-Key': api_key,
		'X-RapidAPI-Host': 'crime-data-by-zipcode-api.p.rapidapi.com'
	}
	results = []
	for zip in zipcodes:
		response = requests.get(url=url, headers=headers, params={'zip': zip})
		if response is not None:
			try:
				data = response.json()
				if data['success'] == True:
					results.append(response.json())
			except Exception as e:
				print(e)
	return results

def load_results_to_s3(client, results, bucket_name):
	for i, obj in enumerate(results):
		if len(obj) > 0:
			json_data = json.dumps(obj)
			client.put_object(
				Bucket=bucket_name, 
				Key=f'real_estate/crime/{date.today()}/result_{i}.json',
				Body=json_data
			)

def main(event, context):
	bucket_name = os.getenv('BUCKET_NAME')
	api_key = os.getenv('RAPID_API_KEY')
	client = boto3.client(
		's3', 
		endpoint_url='https://s3.amazonaws.com',
		aws_access_key_id=os.getenv('ACCESS_KEY'),
		aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY')
	)
	conn = snowflake_connector.connect(
		user=os.getenv('SNOWFLAKE_USERNAME'),
		password=os.getenv('SNOWFLAKE_PASSWORD'),
		account=os.getenv('SNOWFLAKE_ACCOUNT'),
		warehouse=os.getenv('WAREHOUSE'),
		database=os.getenv('DATABASE'),
		schema=os.getenv('SCHEMA')
	)
	zipcodes = get_list_of_zip_codes(conn)
	results = get_results_for_zipcodes(zipcodes, api_key)
	load_results_to_s3(client, results, bucket_name)
	return {'statusCode': 200}
