import streamlit as st
import pandas as pd
#import plotly.graph_objects as go
import math

import sys
import os


# Add the project root to the system path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if project_root not in sys.path:
	sys.path.append(project_root)

from functions import *

#df = load_data("../data/clean/complete.csv") # for local development
#with st.sidebar:
selected_unit = st.pills(
	label=None,
	options=['30 Days', '90 Days', '1 Year', 'Year To Date'],
	selection_mode='single',
	default='Year To Date'
	)
if selected_unit == '30 Days':
	unit = 30
elif selected_unit == '90 Days':
	unit = 90
elif selected_unit == '1 Year':
	unit = 365
else:
	unit = None

df, vix_df = load_data(unit)
sp_growth = df.loc[df['symbol'] == '^GSPC'][['date', 'changePercent']].copy()

groups = df.groupby('symbol')

symbols_list = df['symbol'].unique()

num_rows = math.ceil(len(symbols_list) / 2)

# Create grid
for row in range(num_rows):
	# Create two columns
	col1, col2 = st.columns(2)
	
	# First item in row
	with col1:
		with st.container(border=True):
			if row * 2 < len(symbols_list):
				symbol = symbols_list[row * 2]
				group = groups.get_group(symbol).copy()
				st.plotly_chart(plot_candles(group, symbol, sp_growth))
	
	# Second item in row (if exists)
	with col2:
		with st.container(border=True):
			if row * 2 + 1 < len(symbols_list):
				symbol = symbols_list[row * 2 + 1]
				group = groups.get_group(symbol).copy()
				st.plotly_chart(plot_candles(group, symbol, sp_growth))

st.table(df.head())
