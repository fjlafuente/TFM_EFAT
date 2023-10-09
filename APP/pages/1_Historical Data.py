import streamlit as st
import pandas as pd
import numpy as np
import time
import tempfile
from EFAT_Functions import drive_read_file, read_gpd_file, filter_consolidated_df, create_spain_map, translate_canarias, plot_map_generation, plot_map_demand,color_theme

st.set_page_config(
    page_title='EFAT - Historical Data'
)

st.markdown("""
    ### Historical Data

    The EFAT project is all about predicting energy generation and demand using weather forecasts. Before diving into the heavy stuff, we've got to see how our model's variables stack up and how they connect.
    Here, you can play around with charts that show electricity trends over the year and differences between the Comunidades Autónomas. It's a simple way to see which regions are leaning into certain energy technologies.
    """)

url_df_total_processed = 'https://drive.google.com/file/d/1Ctk3DVWuoxd_ELWqlm0AamL5Was48E4B/view?usp=drive_link'
solar_fotovoltaica = 'https://drive.google.com/file/d/1ucHkMuuAjg1NNd-Ij_lFXrTsL3fhHOus/view?usp=drive_link'
solar_termica = 'https://drive.google.com/file/d/165YUcWEGe0RnzbHJ2xoyKl3SupqXcgR8/view?usp=drive_link'
eolica = 'https://drive.google.com/file/d/13c0eGV2rDa8rh93S5OP8fwCsgoM8q-lR/view?usp=drive_link'
hidraulica = 'https://drive.google.com/file/d/1VRWosTDLcA4RNshh2MgUTJ_DPJiGofJP/view?usp=drive_link'
demanda = 'https://drive.google.com/file/d/1K9hRTaLJR1473Med7kmTRQ0AULm_Xw-_/view?usp=drive_link'
url_map = 'https://drive.google.com/file/d/1G0YE63B11fjXXNvByeNDO99o0yvdY1uw/view?usp=drive_link'



#In order to have spain map defined
@st.cache_data
def load_spain_map(url):
    spain_map = read_gpd_file(url)
    return spain_map

@st.cache_data
def process_spain_map(_spain_map):
    spain_map = create_spain_map(_spain_map)
    spain_map = translate_canarias(spain_map)
    return spain_map

@st.cache_data
def load_processed_data(url):
    df = drive_read_file(url)
    df['fecha'] = pd.to_datetime(df['fecha'])
    return df

# In order to plot the map
spain = load_spain_map(url_map)
spain = process_spain_map(spain)

# In order to have the df processed with the historical data:
df_processed = load_processed_data(url_df_total_processed)
df_processed['fecha'] = pd.to_datetime(df_processed['fecha'])
#test



#In order to let the user select the year

year = st.select_slider(
    'Select a year',
    (2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022)
)


df_solartermica, df_solarfotovoltaica, df_hidraulica, df_eolica, df_demand = filter_consolidated_df(df_processed)

#Let the user select the df to see
st.sidebar.header("Select data to analyze")
selected_data = st.sidebar.selectbox("Choose a variable", 
                                      ('Solar foto', 'Solar termo', 'Eolic', 'Hidraulic', 'Demand'))


if selected_data == 'Solar foto':
    df = df_solarfotovoltaica
    #IN order to know the y limits for the plots
    dataframelimits = df.copy()
    dataframelimits['year'] = dataframelimits['fecha'].dt.year
    grouped_data = dataframelimits.groupby(['year', 'comunidad_autonoma'])['generation'].sum()
    plotlimits = [grouped_data.min()/1000, grouped_data.max()/1000]

elif selected_data == 'Solar termo':
    df = df_solartermica
    #IN order to know the y limits for the plots
    dataframelimits = df.copy()
    dataframelimits['year'] = dataframelimits['fecha'].dt.year
    grouped_data = dataframelimits.groupby(['year', 'comunidad_autonoma'])['generation'].sum()
    plotlimits = [grouped_data.min()/1000, grouped_data.max()/1000]
elif selected_data == 'Eolic':
    df = df_eolica
    #IN order to know the y limits for the plots
    dataframelimits = df.copy()
    dataframelimits['year'] = dataframelimits['fecha'].dt.year
    grouped_data = dataframelimits.groupby(['year', 'comunidad_autonoma'])['generation'].sum()
    plotlimits = [grouped_data.min()/1000, grouped_data.max()/1000]

elif selected_data == 'Hidraulic':
    df = df_hidraulica
    #IN order to know the y limits for the plots
    dataframelimits = df.copy()
    dataframelimits['year'] = dataframelimits['fecha'].dt.year
    grouped_data = dataframelimits.groupby(['year', 'comunidad_autonoma'])['generation'].sum()
    plotlimits = [grouped_data.min()/1000, grouped_data.max()/1000]

elif selected_data == 'Demand':
    df = df_demand
    #IN order to know the y limits for the plots
    dataframelimits = df.copy()
    dataframelimits['year'] = dataframelimits['fecha'].dt.year
    grouped_data = dataframelimits.groupby(['year', 'comunidad_autonoma'])['demand_ccaa'].sum()
    plotlimits = [grouped_data.min()/1000, grouped_data.max()/1000]

#Import of the color we have created for streamlit  

efat_color = color_theme()

tab1, tab2, tab3 = st.tabs(['Map', 'Year Evolution', 'Data'])

with tab1:
    
    if selected_data == 'Demand':
        st.header(" Energetic demand by CCAA, Spain")
        fig = plot_map_demand(spain, df, year, plotlimits)
        st.pyplot(fig)
    else:
        st.header(" Energetic generation by CCAA, Spain")
        fig = plot_map_generation(spain, df, year, plotlimits)
        st.pyplot(fig)

with tab2:

    if selected_data == 'Demand':

        df_plot = df[df['fecha'].dt.year == year].copy()
        df_plot['month'] = df_plot['fecha'].dt.month
        df_plot = df_plot.groupby('month')['demand_ccaa'].sum().reset_index()
        st.bar_chart(df_plot, x = 'month', y = 'demand_ccaa', color = '#21D67C')

    else:

        df_plot = df[df['fecha'].dt.year == year].copy()
        df_plot['month'] = df_plot['fecha'].dt.month
        df_plot = df_plot.groupby('month')['generation'].sum().reset_index()
        st.bar_chart(df_plot, x = 'month', y = 'generation', color = '#21D67C')

with tab3:

    if selected_data == 'Demand':
        st.header("PLOT Energetic demand by CCAA, Spain")
        df[df['fecha'].dt.year == year]
    else:
        st.header("PLOT Energetic generation by CCAA, Spain")
        df[df['fecha'].dt.year == year]