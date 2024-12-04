import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv
from google.cloud import bigquery


@st.cache_data
def load_data():

	load_dotenv()

	project_id = os.getenv('GCP_PROJECT_ID')

	# Create the client to interface with BigQuery.
	client = bigquery.Client(project=project_id)
	dataset_id = 'stock_data'
	target_table_id = 'raw_stock_data'
	target_table_ref = f"{project_id}.{dataset_id}.{target_table_id}"

	query_string = f"""
		SELECT *
		FROM `{target_table_ref}`
		"""

	query_job = client.query(query_string)
	results = query_job.result()
	df = pd.DataFrame([dict(row) for row in results])
	df_with_gain_loss = calculate_gain_loss(df)
	df_with_rsi = calculate_rsi(df_with_gain_loss)
	df_with_macd = calculate_macd(df_with_rsi)

	return df_with_macd

def plot_candles(df, symbol):

	fig = make_subplots(
		rows=3,
		cols=1,
		row_heights=[.6, .2, .2],
		shared_xaxes=True,
		vertical_spacing=0.1
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
		go.Line(
			x=df['date'],
			y=df['rsi'],
			name='RSI'
		),
		row=2,
		col=1
	)

	fig.add_trace(
		go.Line(
			x=df['date'],
			y=df['macd'],
			name='MACD'
		),
		row=3,
		col=1
	)

	fig.add_trace(
		go.Line(
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
			name='MACD Delta'
		),
		row=3,
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
		type='category'
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
		hovermode='x unified'
	)

	return fig

def calculate_gain_loss(df):
	df['gain'] = np.where(df['change'] > 0, df['change'], 0)
	df['loss'] = np.abs(np.where(df['change'] < 0, df['change'], 0))
	return df

def calculate_rsi(df):
	rsi_results = []
	for _, group in df.groupby('symbol'):
		group.sort_values(by='date', inplace=True)
		average_gain = group['gain'].ewm(span=14, adjust=False).mean()
		average_loss = group['loss'].ewm(span=14, adjust=False).mean()
		rs = average_gain / average_loss
		rsi = 100 - (100 / (1 + rs))
		group['rsi'] = rsi
		rsi_results.append(group)
	return pd.concat(rsi_results)

def calculate_macd(df):
	macd_results = []
	for _, group in df.groupby('symbol'):
		group.sort_values(by='date', inplace=True)
		fast = group['close'].ewm(span=12, adjust=False).mean()
		slow = group['close'].ewm(span=26, adjust=False).mean()
		group['macd'] = fast - slow
		group['signal'] = group['macd'].ewm(span=9, adjust=False).mean()
		group['macdHist'] = group['macd'] - group['signal']
		macd_results.append(group)
	return pd.concat(macd_results)
