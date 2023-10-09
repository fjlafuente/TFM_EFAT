import streamlit as st
import pandas as pd
import numpy as np
import time
import tempfile
from EFAT_Functions import drive_read_image
from PIL import Image

url_image = "https://drive.google.com/file/d/19etzlInRxIav6jefr_mhdEWMm4monkQQ/view?usp=drive_link"


st.set_page_config(
    page_title='EFAT - Data Science Project'
)

st.markdown("""
    ### Electricity Forecasting and Analysis Tool
    
    This Streamlit app is a component of a Data Science project at K-School known as EFAT. The main objective of this project is to develop a tool for predicting renewable energy production based on weather forecasting.

    Given the accelerating impact of global warming worldwide, it is imperative to identify and promote green energy resources to mitigate its consequences as much as possible. In a context where fluctuations in electricity prices often causes macroeconomic trends and periods of inflation, gaining a better understanding of these green energy resources becomes essential.

    Within this app, you will discover historical data for Spain that combines weather information with energy generation from 2015 to 2022. Furthermore, as part of the analysis, the tool offers a demonstration on the 'Predictions' page, allowing you to forecast renewable energy production for the upcoming week, taking into account the weather forecast and the current status of water reservoirs across the entire country.

    Hope you enjoy it and find it useful!
    """)
image = drive_read_image(url_image)
st.image(image, use_column_width = 'always')