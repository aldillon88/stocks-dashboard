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

#df = load_data("../data/clean/complete.csv") # for local development
df = load_data("insert path to csv")
st.text(df)



