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


def process_data(apikey, api_lookup, client, table_id):
	"""
	Processes stock data for a list of symbols and inserts it into a BigQuery table.

	Args:
		apikey (str): Your API key for authenticating with the Financial Modeling Prep API.
		api_lookup (list): A list of tuples, where each tuple contains:
			- The stock ticker symbol (str) to retrieve data for (e.g., 'AAPL').
			- The start date (str) in 'YYYY-MM-DD' format to retrieve historical data from.

	Side Effects:
		- Retrieves historical stock price data using the `retrieve_data` function.
		- Adds a timestamp to each record and appends the symbol to the retrieved data.
		- Inserts the processed data into a BigQuery table specified by the global `table_id` variable.
		- Prints log messages for debugging, including retrieved rows and any insertion errors.

	Returns:
		None

	Example:
		>>> api_lookup = [('AAPL', '2023-01-01'), ('GOOG', '2022-01-01')]
		>>> process_data('your_api_key', api_lookup)
		rows_to_insert:
		 [{'symbol': 'AAPL', 'date': '2023-01-02', 'timestamp': '2024-11-23T15:00:00Z', ...}]
		Data inserted successfully.

	Notes:
		- This function relies on the `retrieve_data` function to fetch data from the Financial Modeling Prep API.
		- Each data record is enriched with:
			- The `symbol` field for the stock ticker.
			- The current timestamp in ISO format (`datetime.now().isoformat()`).
		- Ensure the `table_id` variable is defined globally with the correct BigQuery table identifier.
		- Error handling is minimal; consider adding robust exception handling for API failures, empty results, and BigQuery errors.
		- Print statements are used for debugging; they may be removed or replaced with a proper logging framework in production.

	Dependencies:
		- Requires the `retrieve_data` function for fetching stock data.
		- Requires a configured BigQuery `client` object and `table_id` variable for database interactions.
		- Uses Python's `datetime` module for timestamp generation.

	"""
	for item in api_lookup:
		symbol = item[0]
		from_date = item[1]
		logging.info(f"(process_data) The from_date for {symbol} is: {from_date}")
		stock_data = retrieve_data(apikey, symbol, from_date)
		
		if stock_data:
			for data in stock_data['historical']:
				data['symbol'] = stock_data['symbol']
				data['timestamp'] = datetime.now().isoformat()
			rows_to_insert = stock_data['historical']
			errors = client.insert_rows_json(table_id, rows_to_insert)
			
			if errors == []:
				print("Data inserted successfully.")
			else:
				print("Encountered errors while inserting rows:", errors)

		else:
			print('stock_data is empty')


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
	db = 'stock_data'
	table = 'raw_stock_data'
	table_id = f"{project_id}.{db}.{table}"

	# List the stock symbols for which data is to be retrieved.
	symbols = ['AAPL', 'TTD', 'GOOG', 'DDOG', 'PANW']

	results = query_bq(client, table_id, symbols)
	api_lookup = create_api_lookup(results)
	process_data(apikey, api_lookup, client, table_id)
	return "Process complete"

if __name__ == '__main__':
	main()