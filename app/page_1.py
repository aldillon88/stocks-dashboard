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
df = load_data()
st.table(df.head(5))

groups = df.groupby('symbol')

symbols_list = df['symbol'].unique()
st.text(symbols_list)

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
				group = groups.get_group(symbol)
				st.plotly_chart(plot_candles(group, symbol))
	
	# Second item in row (if exists)
	with col2:
		with st.container(border=True):
			if row * 2 + 1 < len(symbols_list):
				symbol = symbols_list[row * 2 + 1]
				group = groups.get_group(symbol)
				st.plotly_chart(plot_candles(group, symbol))


st.plotly_chart(test())