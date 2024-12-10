import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv
from google.cloud import bigquery
import datetime
import yfinance as yf


@st.cache_data
def load_data(unit=None):

	load_dotenv()

	project_id = os.getenv('GCP_PROJECT_ID')

	# Create the client to interface with BigQuery.
	client = bigquery.Client(project=project_id)
	dataset_id = 'stock_data'
	target_table_id = 'raw_stock_data'
	target_table_ref = f"{project_id}.{dataset_id}.{target_table_id}"

	query_string = f"""
		SELECT `adjClose`, `change`, `changePercent`, `close`, `date`, `high`, `low`, `open`, `symbol`, `volume`
		FROM `{target_table_ref}`
		"""

	query_job = client.query(query_string)
	results = query_job.result()
	df = pd.DataFrame([dict(row) for row in results])
	start_date = df['date'].min()
	end_date = df['date'].max()
	print(f"start_date: {start_date}\nend_date: {end_date}")
	sp = get_sp500_historical_prices(start_date, end_date)
	df = pd.concat([df, sp])

	df = add_metrics(df)
	print(df.dtypes)
	df['date'] = pd.to_datetime(df['date'])
	print(df.dtypes)

	vix_df = get_historical_vix(start_date, end_date)

	if unit:
		df = df.loc[df['date'] > df['date'].max() - datetime.timedelta(unit)]
		vix_df = vix_df.loc[vix_df['date'] > vix_df['date'].max() - datetime.timedelta(unit)]

	return df, vix_df

def get_sp500_historical_prices(start_date, end_date):
	"""
	Fetch historical S&P 500 prices using yfinance.
	
	Args:
		start_date (str): Start date in the format 'YYYY-MM-DD'.
		end_date (str): End date in the format 'YYYY-MM-DD'.
	
	Returns:
		pd.DataFrame: Historical price data for the S&P 500.
	"""
	# Define the S&P 500 ticker
	sp500_ticker = "^GSPC"
	
	# Fetch the data
	sp500_data = yf.download(sp500_ticker, start=start_date, end=end_date)
	sp500_data['symbol'] = sp500_ticker

	if isinstance(sp500_data.columns, pd.MultiIndex):
		sp500_data.columns = sp500_data.columns.get_level_values(0)
	
	sp500_data = sp500_data.reset_index()
	sp500_data.columns = sp500_data.columns.str.lower()
	sp500_data = sp500_data.rename(columns={'adj close': 'adjClose'})
	sp500_data['change'] = sp500_data['close'].diff().bfill()
	sp500_data['date'] = sp500_data['date'].dt.strftime('%Y-%m-%d')
	
	return sp500_data

def get_historical_vix(start_date, end_date):
	ticker = yf.Ticker('^VIX')
	vix_df = pd.DataFrame(ticker.history(start=start_date, end=end_date)).reset_index()
	vix_df = vix_df.drop(columns=['Volume', 'Dividends', 'Stock Splits'])
	vix_df.columns = vix_df.columns.str.lower()
	return vix_df

@st.cache_data
def get_ticker_summary(symbols):

	cols_to_keep = [
		'symbol', 'shortName', 'beta', 'trailingPE', 'forwardPE', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'priceToSalesTrailing12Months', 'profitMargins',
		'trailingEps', 'forwardEps', 'currentPrice', 'targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice',
		'recommendationKey', 'earningsGrowth', 'revenueGrowth'
	]

	summary_list = []

	for symbol in symbols:
		ticker = yf.Ticker(symbol)
		summary_list.append(ticker.info)
	df = pd.DataFrame(summary_list)[cols_to_keep]
	df['impliedReturn'] = round((df['targetMeanPrice'] - df['currentPrice']) / df['currentPrice'], 2)
	
	return df

def add_metrics(df):

	metrics_list = []

	# Iterate through each symbol and calculate metrics.
	for _, group in df.groupby('symbol'):

		group = group.sort_values(by='date').copy()

		# Add gain, loss and changePercent.
		g1 = calculate_gain_loss(group)

		# Add RSI
		g2 = calculate_rsi(g1)

		# Add MACD
		g3 = calculate_macd(g2)

		# Append the enriched dataframe to the metrics_list.
		metrics_list.append(g3)

	return pd.concat(metrics_list)

def calculate_gain_loss(df):
	df['gain'] = np.where(df['change'] > 0, df['change'], 0)
	df['loss'] = np.abs(np.where(df['change'] < 0, df['change'], 0))
	df['changePercent'] = df['close'].pct_change().fillna(0)
	return df

def calculate_rsi(df):

	average_gain = df['gain'].ewm(span=14, adjust=False).mean()
	average_loss = df['loss'].ewm(span=14, adjust=False).mean()
	rs = average_gain / (average_loss + 1e-10)
	rsi = 100 - (100 / (1 + rs))
	df['rsi'] = rsi
	return df

def calculate_macd(df):

	fast = df['close'].ewm(span=12, adjust=False).mean()
	slow = df['close'].ewm(span=26, adjust=False).mean()
	df['macd'] = fast - slow
	df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
	df['macdHist'] = df['macd'] - df['signal']
	return df

def plot_candles(df, symbol, sp_growth):

	df['date'] = df['date'].dt.strftime('%Y-%m-%d')
	sp_growth['date'] = pd.to_datetime(sp_growth['date'])
	sp_growth['date'] = sp_growth['date'].dt.strftime('%Y-%m-%d')

	fig = make_subplots(
		rows=4,
		cols=1,
		row_heights=[.4, .2, .2, .2],
		shared_xaxes=True,
		vertical_spacing=0.07
	)

	fig.add_trace(
		go.Candlestick(
			x=df['date'],
			open=df['open'],
			high=df['high'],
			low=df['low'],
			close=df['close'],
			name='Price'
		),
		row=1,
		col=1
	)

	fig.add_trace(
		go.Scatter(
			x=df['date'],
			y=df['rsi'],
			name='RSI'
		),
		row=2,
		col=1
	)

	fig.add_trace(
		go.Scatter(
			x=df['date'],
			y=df['macd'],
			name='MACD',
		),
		row=3,
		col=1
	)

	fig.add_trace(
		go.Scatter(
			x=df['date'],
			y=df['signal'],
			name='Signal'
		),
		row=3,
		col=1
	)

	fig.add_trace(
		go.Bar(
			x=df['date'],
			y=df['macdHist'].astype('float'),
			name='Delta'
		),
		row=3,
		col=1
	)

	fig.add_trace(
		go.Scatter(
			x=df['date'],
			y=df['changePercent'].cumsum(),
			name=symbol
		),
		row=4,
		col=1
	)

	fig.add_trace(
		go.Scatter(
			x=sp_growth['date'],
			y=sp_growth['changePercent'].cumsum(),
			name='S&P 500'
		),
		row=4,
		col=1
	)

	fig.update_yaxes(
		range=[0, 100],
		row=2,
		col=1
		)
	
	fig.update_xaxes(
		rangebreaks=[
			dict(
				bounds=['sat', 'mon']
			)
		],
		type='category',
		tickangle=-45,
		tickformat='%Y-%m-%d'
	)
	
	fig.add_hline(
		y=30,
		line_dash='dot',
		line_color='green',
		label=dict(
			text='Oversold',
			textposition='start',
			font=dict(
				size=10,
				color='green'
			),
			yanchor='top'
		),
		row=2,
		col=1
	)

	fig.add_hline(
		y=70,
		line_dash='dot',
		line_color='red',
		label=dict(
			text='Overbought',
			textposition='start',
			font=dict(
				size=10,
				color='red'
			),
			yanchor='bottom'
		),
		row=2,
		col=1
	)

	fig.update_layout(
		title=dict(
			text=symbol,
			x=0,
			y=1,
			xanchor='left',
			yanchor='top'
		),
		margin=dict(
			l=0,
			r=0,
			t=50,
			b=0
		),
		xaxis_rangeslider_visible=False,
		showlegend=False,
		yaxis_tickformat='$',
		yaxis=dict(
			title='Price',
			titlefont=dict(size=12)
		),
		yaxis2=dict(
			title='RSI',
			titlefont=dict(size=12)
		),
		yaxis3=dict(
			title='MACD',
			titlefont=dict(size=12)
		),
		yaxis4=dict(
			title='vs. S&P 500',
			titlefont=dict(size=12),
			tickformat='.0%'
		),
		hovermode='x unified'
	)

	return fig

def plot_vs_sp(df, symbols=None):

	#df = df.loc[df['symbol'] == '^GSPC']

	fig = go.Figure()

	for symbol, group in df.groupby('symbol'):

		fig.add_trace(
			go.Scatter(
				x=group['date'],
				y=group['changePercent'].cumsum(),
				name=symbol
			)
		)

		fig.update_layout(
			title=dict(
				text='% Growth',
				x=0,
				y=1,
				xanchor='left',
				yanchor='top'
			),
			margin=dict(
				l=0,
				r=0,
				t=50,
				b=0
			),
			xaxis_rangeslider_visible=False,
			showlegend=True,
			yaxis=dict(
				title='% Growth',
				titlefont=dict(size=12),
				tickformat='.0%'
			),
			hovermode='x unified'
	)

	return fig

def plot_target_price(keys, values):
	fig = go.Figure()
	fig.add_trace(
		go.Scatter(
			x = values,
			y = np.zeros(len(values)),
			mode='markers+text',
			marker=dict(
				size=15
			)
		)
	)

	for key, val in zip(keys, values):
		fig.add_annotation(
			x=val,
			y=0,
			text=key,
			textangle=45,
			xanchor='right',
			showarrow=False,
			yshift=25
		)
	
	fig.update_yaxes(
		zeroline=True,
		zerolinewidth=(2),
		zerolinecolor='LightPink',
		showgrid=False,
		tickmode='array',
		tickvals=[0]
	)

	fig.update_layout(
		autosize=True,
		height=200
	)

	return fig

def plot_vix(vix_df):
	fig = go.Figure()
	fig.add_trace(go.Scatter(
		x=vix_df['date'],
		y=vix_df['close'],
		fill='tozeroy',
		mode='lines'
	))
	return fig