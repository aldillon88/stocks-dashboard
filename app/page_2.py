import streamlit as st
import pandas as pd

import sys
import os

# Add the project root to the system path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if project_root not in sys.path:
	sys.path.append(project_root)

from functions import *

df = load_data()

symbols = df['symbol'].unique().tolist()
symbols.remove('^GSPC')
selected_symbol = st.selectbox(label='Select a stock symbol', options=symbols)
selected_symbols = [selected_symbol, '^GSPC']
df_filtered = df.loc[df['symbol'].isin(selected_symbols)].copy()
st.plotly_chart(plot_vs_sp(df_filtered))

col1, col2 = st.columns([.2, .8])



import yfinance as yf
ticker = yf.Ticker('AAPL')
st.write(ticker.info['city'])
profile = ticker.info
# beta, trailingPE, forwardPE, fiftyTwoWeekLow, fiftyTwoWeekHigh, priceToSalesTrailing12Months, profitMargins
# trailingEps, forwardEps, currentPrice, targetHighPrice, targetLowPrice, targetMeanPrice, targetMedianPrice
# recommendationKey, earningsGrowth, revenueGrowth
balance_sheet = ticker.balance_sheet
st.json(ticker.info)
st.table(ticker.balance_sheet)
st.write(ticker.analyst_price_targets)
st.write(ticker.earnings_estimate)
st.write(ticker.earnings_history)
st.write(ticker.eps_trend)
st.write(ticker.recommendations)
















