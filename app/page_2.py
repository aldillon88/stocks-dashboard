import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
import os

# Add the project root to the system path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if project_root not in sys.path:
	sys.path.append(project_root)

from functions import *

#with st.container():
st.header("Overview")
st.markdown("""
	Insert text!
	""")

st.divider()
st.header("Sample Header")
st.markdown("""
	Insert text!
	""")

















