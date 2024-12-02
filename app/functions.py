import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
from dotenv import load_dotenv
from google.cloud import bigquery


@st.cache_data
def load_data(symbol, table_id):

	load_dotenv()

	project_id = os.getenv('GCP_PROJECT_ID')

	# Create the client to interface with BigQuery.
	client = bigquery.Client(project=project_id)
	dataset_id = 'stock_data'
	target_table_id = 'raw_stock_data'
	target_table_ref = f"{project_id}.{dataset_id}.{target_table_id}"

	query_string = f"""
		SELECT *
		FROM {table_id}
		WHERE `symbol` = {symbol}
		"""

	query_job = client.query(query_string)
	results = query_job.result()

	return results
