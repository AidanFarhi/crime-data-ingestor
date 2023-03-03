import os
import boto3
import requests
import json
import snowflake.connector as snowflake_connector
from dotenv import load_dotenv
from datetime import date
load_dotenv()


def get_list_of_zip_codes():
	conn_params = {
		'user': os.getenv('SNOWFLAKE_USERNAME'),
		'password': os.getenv('SNOWFLAKE_PASSWORD'),
		'account': os.getenv('SNOWFLAKE_ACCOUNT'),
		'warehouse': 'COMPUTE_WH',
		'database': 'DEV_DATABASE',
		'schema': 'PUBLIC',
	}
	conn = snowflake_connector.connect(**conn_params)
	cursor = conn.cursor()
	cursor.execute("SELECT zip FROM ZIPCODE WHERE state = 'DE'")
	results = (r[0] for r in cursor.fetchall())
	conn.close()
	return results

def get_results_for_zipcodes(zipcodes):
	url = "https://crime-data-by-zipcode-api.p.rapidapi.com/crime_data"
	headers = {
		'X-RapidAPI-Key': os.getenv('RAPID_API_KEY'),
		"X-RapidAPI-Host": "crime-data-by-zipcode-api.p.rapidapi.com"
	}
	results = []
	for zip in zipcodes:
		response = requests.get(url=url, headers=headers, params={'zip': zip})
		results.append(response.json())
	return results

def load_results_to_s3(results):
	client = boto3.client(
	    's3', 
	    endpoint_url='https://s3.amazonaws.com',
	    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
	    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
	)
	for i, obj in enumerate(results):
		if len(obj) > 0:
			json_data = json.dumps(obj)
			client.put_object(
				Bucket=os.getenv('BUCKET_NAME'), 
				Key=f'real_estate/crime/{date.today()}/result_{i}.json',
				Body=json_data
			)

def main():
	zipcodes = get_list_of_zip_codes()
	results = get_results_for_zipcodes(zipcodes)
	load_results_to_s3(results)


if __name__ == '__main__':
	main()
