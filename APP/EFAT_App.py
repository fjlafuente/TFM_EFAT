import streamlit as st
import pandas as pd
import numpy as np
import time

st.set_page_config(
    page_title='EFAT - Data Science Project',
)

st.markdown("""
    ### Electricity Forecasting and Analysis Tool
    
    This Streamlit app is part of a Data Science project called EFAT. The aim of the project is to provide a tool to predict renewable energy production.
    In the app, you will find historical data for Spain, merging weather information with energy generation from 2015 to 2022.
    Based on this analysis, the tool also provides a demo to test what the renewable energy production will be, taking into consideration the weather forecast for the next week across the entire country.
    Hope you enjoy!
    """)

