from google.cloud import bigquery
import urllib.request
import urllib.parse
import urllib.error
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import time
import logging

#logger = logging.getLogger(__name__)

def query_bq(client, table_id, symbols):
	# Query BigQuery to list the symbol and corresponding last date in the table.
	# This will be used to generate the from_date parameter for the API call.
	query_string = f"""
		WITH symbol_list AS (
			SELECT symbol
			FROM UNNEST(@symbols) AS symbol
		)
		SELECT symbol_list.symbol, MAX(stock_data.`date`) AS `date`
		FROM symbol_list
		LEFT JOIN `{table_id}` AS stock_data
		ON symbol_list.symbol = stock_data.`symbol`
		GROUP BY symbol_list.symbol
	"""

	job_config = bigquery.QueryJobConfig(
			query_parameters=[
				bigquery.ArrayQueryParameter('symbols', 'STRING', symbols)
			]
		)
	query_job = client.query(query_string, job_config=job_config)
	results = query_job.result()

	return results

def create_api_lookup(query_results):
	# Generate a temporary list to hold the final parameters for the API calls:
	# i.e. [['GOOG', '2024-01-01'], ['AAPL', '2024-03-31']]
	api_lookup = []
	default_date = '2024-11-19'
	today = datetime.now().strftime(format='%Y-%m-%d')

	for row in query_results:

		if row['date']:

			if str(row['date']) == today:
				r = [
					row['symbol'],
					today
				]
			
			else:
				date_plus_1 = row['date'] + timedelta(days=1)
				r = [
					row['symbol'],
					datetime.strftime(date_plus_1, format='%Y-%m-%d')
				]

		else:
			r = [
				row['symbol'],
				default_date
			]

		api_lookup.append(r)
	
	return api_lookup



# Functions to generate the API url, retrieve and process the data.
def historical_url(apikey, ticker, from_date=None):
	"""
	Constructs a URL to retrieve historical stock price data from the Financial Modeling Prep API.

	Args:
		apikey (str): Your API key for authenticating with the Financial Modeling Prep API.
		ticker (str): The stock ticker symbol for which historical data is requested (e.g., 'AAPL').
		from_date (str, optional): The start date for the historical data in 'YYYY-MM-DD' format. 
								   Defaults to None, in which case the API will return all available data.

	Returns:
		str: A complete URL string to query the Financial Modeling Prep API for the specified stock and date range.

	Example:
		>>> historical_url('your_api_key', 'AAPL', '2020-01-01')
		'https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey=your_api_key&from=2020-01-01'
	"""

	url = 'https://financialmodelingprep.com/api/v3/historical-price-full/'

	query_params = {
		'apikey': apikey
	}

	if from_date is not None:
		query_params['from'] = from_date

	encoded_params = urllib.parse.urlencode(query_params)

	full_url = f"{url}{ticker}?{encoded_params}"

	return full_url


def retrieve_data(apikey, symbol, from_date, max_retries=3, delay=2):
	"""
	Retrieves historical stock price data from the Financial Modeling Prep API for a specific stock symbol and date range.

	Args:
		apikey (str): Your API key for authenticating with the Financial Modeling Prep API.
		symbol (str): The stock ticker symbol for which historical data is requested (e.g., 'AAPL').
		from_date (str): The start date for the historical data in 'YYYY-MM-DD' format.

	Returns:
		dict: A dictionary containing the parsed JSON response from the API, which includes the historical stock price data.

	Side Effects:
		- Prints the constructed URL used for the API call.
		- Prints the data retrieved from the API for debugging purposes.

	Example:
		>>> retrieve_data('your_api_key', 'AAPL', '2020-01-01')
		The URL is: https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey=your_api_key&from=2020-01-01
		The data is: {...}
		{...}

	Notes:
		- This function uses the `historical_url` function to construct the API URL.
		- Make sure the API key, ticker symbol, and date format are valid to avoid runtime errors.
		- This function reads the API response and parses it as JSON. Ensure that the API endpoint is reachable, and the response format matches expectations.
	"""
	url = historical_url(apikey, symbol, from_date)
	for attempt in range(max_retries):
		
		try:
			response = urllib.request.urlopen(url)
			data = json.loads(response.read())
			logging.info(f"Successfully retrieved data for {symbol} from {url}")
			return data
		
		except urllib.error.URLError as e:
			logging.warning(f"Attempt {attempt + 1} failed with reason: {e.reason}")
			logging.warning(f"URL: {url}")
			if attempt < max_retries - 1:
				time.sleep(delay)
			else:
				logging.error(f"Max retries reached for URL: {url}")
				raise # Re-raise the exception if retries are exhausted

		except urllib.error.HTTPError as e:
			logging.warning(f"Attempt {attempt + 1} failed with error: {e.code} {e.reason}")
			if attempt < max_retries - 1:
				time.sleep(delay)
			else:
				logging.error(f"Max retries reached for HTTP error: {e.code} {e.reason}")
				raise

		except Exception as e:
			logging.error(f"Attempt {attempt + 1} failed with unexpected error: {e}")
			if attempt < max_retries - 1:
				time.sleep(delay)
			else:
				logging.error(f"Max retries reached for unexpected error: {e}")
				raise


def merge_table(client, target_table_ref, temp_table_ref):
	merge_query = f"""
	MERGE INTO `{target_table_ref}` AS target
	USING `{temp_table_ref}` AS source
	ON target.symbol = source.symbol AND target.date = source.date
	WHEN MATCHED THEN
	UPDATE SET
		symbol = source.symbol,
		date = source.date,
		open = source.open,
		high = source.high,
		low = source.low,
		close = source.close,
		adjClose = source.adjClose,
		volume = source.volume,
		unadjustedVolume = source.unadjustedVolume,
		change = source.change,
		changePercent = source.changePercent,
		vwap = source.vwap,
		label = source.label,
		changeOverTime = source.changeOverTime,
		timestamp = source.timestamp
	WHEN NOT MATCHED THEN
	INSERT (
		symbol, 
		date, 
		open, 
		high, 
		low, 
		close, 
		adjClose, 
		volume, 
		unadjustedVolume, 
		change, 
		changePercent, 
		vwap, 
		label, 
		changeOverTime, 
		timestamp
	)
	VALUES (
		source.symbol, 
		source.date, 
		source.open, 
		source.high, 
		source.low, 
		source.close, 
		source.adjClose, 
		source.volume, 
		source.unadjustedVolume, 
		source.change, 
		source.changePercent, 
		source.vwap, 
		source.label, 
		source.changeOverTime, 
		source.timestamp
	)
	"""

	# Execute the query
	query_job = client.query(merge_query)
	query_job.result() 


def process_data(apikey, api_lookup, client, project_id, target_table_ref):
	dataset_id = 'stock_data'

	for item in api_lookup:
		symbol = item[0]
		from_date = item[1]
		logging.info(f"(process_data) Processing data for {symbol} from {from_date}")

		stock_data = retrieve_data(apikey, symbol, from_date)
		if stock_data:
			# Add symbol and timestamp to each row
			for data in stock_data['historical']:
				data['symbol'] = stock_data['symbol']
				data['timestamp'] = datetime.now().isoformat()

			# Create a unique temporary table
			temp_table_id = f"temp_table_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
			temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"

			job_config = bigquery.LoadJobConfig(
				schema=[
					bigquery.SchemaField('symbol', 'STRING'),
					bigquery.SchemaField('date', 'DATE'),
					bigquery.SchemaField('open', 'FLOAT'),
					bigquery.SchemaField('high', 'FLOAT'),
					bigquery.SchemaField('low', 'FLOAT'),
					bigquery.SchemaField('close', 'FLOAT'),
					bigquery.SchemaField('adjClose', 'FLOAT'),
					bigquery.SchemaField('volume', 'INTEGER'),
					bigquery.SchemaField('unadjustedVolume', 'INTEGER'),
					bigquery.SchemaField('change', 'FLOAT'),
					bigquery.SchemaField('changePercent', 'FLOAT'),
					bigquery.SchemaField('vwap', 'FLOAT'),
					bigquery.SchemaField('label', 'STRING'),
					bigquery.SchemaField('changeOverTime', 'FLOAT'),
					bigquery.SchemaField('timestamp', 'TIMESTAMP'),
				],
				write_disposition='WRITE_TRUNCATE'
			)

			try:
				# Insert data into BigQuery
				logging.info(f"Loading data for {symbol} into temporary table: {temp_table_ref}")
				client.load_table_from_json(stock_data['historical'], temp_table_ref, job_config=job_config).result()
				merge_table(client, target_table_ref, temp_table_ref)
				logging.info(f"Data successfully loaded for {symbol}.")

			except Exception as e:
				logging.error(f"Error inserting data for {symbol}: {e}")

			finally:
				# Cleanup the temporary table
				logging.info(f"Deleting temporary table: {temp_table_ref}")
				client.delete_table(temp_table_ref, not_found_ok=True)

		else:
			logging.warning(f"No data retrieved for {symbol} from {from_date}.")



def main():
	# Load the environment variables from the .env file (development only).
	# Use environment variables set on the system in production.
	load_dotenv()

	logging.basicConfig(level=logging.DEBUG)

	# Retrieve the environment variables for the function.
	apikey = os.getenv('FMP_API_KEY')
	project_id = os.getenv('GCP_PROJECT_ID')

	# Create the client to interface with BigQuery.
	client = bigquery.Client(project=project_id)
	dataset_id = 'stock_data'
	target_table_id = 'raw_stock_data'
	target_table_ref = f"{project_id}.{dataset_id}.{target_table_id}"

	# List the stock symbols for which data is to be retrieved.
	symbols = ['AAPL', 'TTD', 'GOOG', 'DDOG', 'PANW']

	results = query_bq(client, target_table_ref, symbols)
	api_lookup = create_api_lookup(results)
	process_data(apikey, api_lookup, client, project_id, target_table_ref)
	
	return "Process complete"

if __name__ == '__main__':
	main()
