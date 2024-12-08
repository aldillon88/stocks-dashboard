import streamlit as st
import pandas as pd

import sys
import os

# Add the project root to the system path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if project_root not in sys.path:
	sys.path.append(project_root)

from functions import *

# Load historical price data from into a dataframe.
price_df = load_data()

# Create a drop-down select box to choose a stock symbol.
symbols = price_df['symbol'].unique().tolist()
symbols.remove('^GSPC')
selected_symbol = st.selectbox(label='Select a stock symbol', options=symbols)

# Create a layout for the top row.
r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns([.3, .15, .15, .15, .15,])

# Filter the dataframe based on the selected symbol and the S&P 500 symbol for comparison.
selected_symbols = [selected_symbol, '^GSPC']
df_filtered = price_df.loc[price_df['symbol'].isin(selected_symbols)].copy()

# Display a chart showing percentage growth over time compared to the S&P 500.
st.plotly_chart(plot_vs_sp(df_filtered))

# Load stock summary data into a dataframe.
summary_df = get_ticker_summary(symbols)
summary_df_filtered = summary_df.loc[summary_df['symbol'] == selected_symbol]

# Add metrics across the top of the dashboard.
with r1c1:
	with st.container(border=True):
		st.metric(label='Company', value=summary_df_filtered['shortName'].values[0])

with r1c2:
	with st.container(border=True):
		value = '${:,.2f}'.format(summary_df_filtered['currentPrice'].values[0])
		st.metric(label='Current Price', value=value)

with r1c3:
	with st.container(border=True):
		value = '${:,.2f}'.format(summary_df_filtered['targetMeanPrice'].values[0])
		st.metric(label='Target Price', value=value)

with r1c4:
	with st.container(border=True):
		value_num = summary_df_filtered['impliedReturn'].values[0]
		value_formatted = format(value_num, '.2%')
		#value = format(summary_df_filtered['impliedReturn'].values[0], '.2%')
		if value_num > 0:
			st.metric(label='Implied Return', value=value_formatted)

with r1c5:
	with st.container(border=True):
		value = summary_df_filtered['recommendationKey'].values[0].replace('_', ' ').capitalize()
		st.metric(label='Recommendation', value=value)

target_price_cols = ['currentPrice', 'targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice']
target_price_vals = summary_df_filtered[target_price_cols].values[0]
target_price_labels = ['Current', 'High', 'Low', 'Mean', 'Median']
st.plotly_chart(plot_target_price(target_price_labels, target_price_vals))

st.table(summary_df_filtered)

import yfinance as yf
ticker = yf.Ticker('AAPL')



st.write(ticker.info['city'])
profile = ticker.info

balance_sheet = ticker.balance_sheet
st.table(ticker.balance_sheet)
st.json(ticker.analyst_price_targets)

st.write(ticker.earnings_estimate)
st.write(ticker.earnings_history)
st.write(ticker.eps_trend)
st.write(ticker.recommendations)












