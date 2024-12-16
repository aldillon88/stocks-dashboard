import streamlit as st
import pandas as pd
import yfinance as yf

import sys
import os

# Add the project root to the system path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if project_root not in sys.path:
	sys.path.append(project_root)

from functions import *

# Load historical price data from into a dataframe.
price_df, vix_df = load_data()

# Create a drop-down select box to choose a stock symbol.
symbols = price_df['symbol'].unique().tolist()
symbols.remove('^GSPC')
selected_symbol = st.selectbox(label='Select a stock symbol', options=symbols)

# Retrieve stock summary info from Yahoo Finance (yfinance)
ticker = yf.Ticker(selected_symbol)

# Create a layout for the top row.
r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns([.3, .15, .15, .15, .15,])

# Filter the dataframe based on the selected symbol and the S&P 500 symbol for comparison.
selected_symbols = [selected_symbol, '^GSPC']
df_filtered = price_df.loc[price_df['symbol'].isin(selected_symbols)].copy()

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
		if abs(value_num) > 0:
			value_formatted = format(value_num, '.2%')
			st.metric(label='Implied Return', value=value_formatted)

with r1c5:
	with st.container(border=True):
		value = summary_df_filtered['recommendationKey'].values[0].replace('_', ' ').capitalize()
		st.metric(label='Recommendation', value=value)


r2c1, r2c2 = st.columns([.7, .3])

with r2c1:
	with st.container(border=True):
		# Display a chart showing percentage growth over time compared to the S&P 500.
		st.plotly_chart(plot_vs_sp(df_filtered))
	with st.container(border=True):
		st.plotly_chart(plot_vix(vix_df))

with r2c2:
	with st.container(border=True):
		reco_df = pd.DataFrame(ticker.recommendations)
		reco_df_pivot = reco_df.set_index('period').T.iloc[:, 0]
		st.plotly_chart(plot_recommendations(reco_df_pivot))

	with st.container(border=True):

		target_price_cols = ['currentPrice', 'targetHighPrice', 'targetLowPrice', 'targetMeanPrice']
		target_price_vals = summary_df_filtered[target_price_cols].values[0]
		target_price_labels = ['Current', 'High', 'Low', 'Mean', 'Median']
		st.plotly_chart(plot_target_price(target_price_labels, target_price_vals))


beta_df = df_filtered.loc[df_filtered['symbol'] == selected_symbol]
with st.container(border=True):
	st.plotly_chart(plot_centered_scatter(beta_df, 'sevenDayBeta'))

with st.container(border=True):
	st.plotly_chart(plot_centered_scatter(beta_df, 'volume'))

st.table(df_filtered)
