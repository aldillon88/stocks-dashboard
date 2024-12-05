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

col1, col2 = st.columns([.2, .8])

with col1:
	symbols = df['symbol'].unique().tolist()
	selected_symbols = st.multiselect(label='Symbols', options=symbols, default='^GSPC')
	df_filtered = df.loc[df['symbol'].isin(selected_symbols)].copy()

with col2:
	st.plotly_chart(plot_vs_sp(df_filtered))


















