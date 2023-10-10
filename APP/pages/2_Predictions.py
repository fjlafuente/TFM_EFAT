import streamlit as st
import pandas as pd
import numpy as np
import time
import tempfile
import datetime
from sklearn.preprocessing import OneHotEncoder
from st_circular_progress import CircularProgress
from EFAT_Functions import aemet_municipios, aemet_municipios_predictions, drive_read_file_othersep, drive_read_latin_encoding_file, drive_read_xlsx_file, drive_read_joblibmodel, download_embalses, sort_municipios
from EFAT_Functions import embalses_select_year, embalses_latest_data, process_df_predictions, cod_provincias, data_REE_potencia_instalada, plot_bars_predictions, plot_completion
from EFAT_Functions import power_installed_last_month, powerinstalled_CCAA_process, pivot_table_generation_by_ccaa, REE_ccaa_rename, all_predictions, fill_na_predictions, standarization_minmax_scaler, filter_predictions_df, onehotencoder_ccaa, apply_model, sort_municipios

st.set_page_config(
    page_title="EFAT_Predictions",
    page_icon="crystall_ball",
    layout="wide",
    initial_sidebar_state="expanded"
) 
st.write('# EFAT Prediction model')

st.markdown(
    """
    Through this page it will be possible to have an estiamtion of the renewable energetic generation - and energetic demand-
    for the next seven days, based on weather forecast provided by AEMET.
    The ML learning models have been training with the historical data from 2015 to 2022. During the preprocess and study phase, 'KNN' & 'RandomForest' were the models with the best performance accross the different technologies. Due t time consumption and resources efficiency, KNN model is the one used in this sheet for predictions.
    """
)


#These are the Google drive urls where we have the data

url_sun = "https://drive.google.com/file/d/1tv7PGRC6tF_R5zoeAfnkzcDb3NKYnXFd/view?usp=drive_link"
url_provincias = "https://drive.google.com/file/d/1892NArUsU5bLbfsqLY1SoePvj5HpA9Tw/view?usp=drive_link"
url_presas = "https://drive.google.com/file/d/1Iyklyh-5QscQBEycehovyTqadiTZSZrI/view?usp=drive_link"
url_embalses = "https://docs.google.com/spreadsheets/d/1rcwa4zVO7rCHH6YjcH37rd5YA5wPUhFw/edit?usp=drive_link&ouid=104422625489291627517&rtpof=true&sd=true"

#In order to apply it to the functions to get latest data:

current_year = datetime.datetime.now().year

#Create the functions with st.cache to store the result in every interaction

@st.cache_data(show_spinner= "Downloading municipalities data...")
def download_municipios ():
    municipios = aemet_municipios()
    municipios = sort_municipios(municipios)
    return municipios

@st.cache_data(show_spinner= "Downloading weather forecast data...")
def download_weather_predictions(municipios):
    df_predictions = aemet_municipios_predictions(municipios)
    return df_predictions

@st.cache_data(show_spinner= "Downloading dams data...")
def download_drive_others(url):
    df = drive_read_file_othersep(url)
    return df


def download_df_embalses(presas_file, year):
    df = download_embalses()
    #We can assume for now it's always going to be 2023
    df = embalses_select_year(df, presas_file, year)
    return df

@st.cache_data(show_spinner= "Downloading provinces data...")
def download_df_provincias(url):
    df = drive_read_file_othersep(url)
    return df

@st.cache_data(show_spinner= "Downloading power installed data...")
def download_power_installed(yearini,yearend):
    df = data_REE_potencia_instalada(yearini, yearend)
    return df

@st.cache_data(show_spinner= "Downloading  data...")
def download_df_drive_latin(url):
    df = drive_read_latin_encoding_file(url)
    return df

@st.cache_data(show_spinner= "Downloading  data...")
def download_model(url):
    model = drive_read_joblibmodel(url)
    return model



#Download the dataframes related to prediction:

progress_text = 'Downloading real-time data, please wait.'
progress = st.progress(0)
percent_complete = 0

st.markdown("""
<style>
.stProgress .st-bo {
    background-color: white;
}
</style>
""", unsafe_allow_html=True)

progress.progress(0/100, text = progress_text)
df_municipios = download_municipios()
df_municipios = sort_municipios(df_municipios)
percent_complete += 5
progress.progress(percent_complete/100, text = progress_text)

df_presas = download_df_drive_latin(url_presas)
percent_complete += 5
progress.progress(percent_complete/100, text = progress_text)

df_predictions = download_weather_predictions(df_municipios)
percent_complete += 25
progress.progress(percent_complete/100, text = progress_text)

df_sun = download_drive_others(url_sun)
percent_complete += 10
progress.progress(percent_complete/100, text = progress_text)

df_embalses = download_df_embalses(df_presas, current_year)
percent_complete += 15
progress.progress(percent_complete/100, text = progress_text)

df_provincias = download_df_drive_latin(url_provincias)
percent_complete += 15
progress.progress(percent_complete/100, text = progress_text)

df_power_installed = download_power_installed(current_year,current_year)
percent_complete += 15
progress.progress(percent_complete/100, text = "Downloading models")

url_solarfoto = "https://drive.google.com/file/d/1TE8Cu3JlCxrAjPFcoYozHkNxG5PwRqkc/view?usp=drive_link"
model_solarfoto = download_model(url_solarfoto)

url_solarterm = "https://drive.google.com/file/d/18lDGDaqJ0xWkvFMWjxEa2Y4FurCW2Ukn/view?usp=drive_link"
model_solarterm = download_model(url_solarterm)

url_eolic = "https://drive.google.com/file/d/1Sp4Sl9vZ0p1j7yIvgxl1JxZC_dF3wVIb/view?usp=drive_link"
model_eolic = download_model(url_eolic)


url_hidro = "https://drive.google.com/file/d/12NXRR5DcgxgCh2kQaV0AWWSpYOH76oJi/view?usp=drive_link"
model_hidro = download_model(url_hidro)

url_demand = "https://drive.google.com/file/d/1krPoisaeSqx8nCXFqyoqeN8F4XkJh4nd/view?usp=drive_link"
model_demand = download_model(url_demand)

percent_complete += 10
progress.progress(percent_complete/100, text = progress_text)

st.success('Download completed')

#Merge and proces the files

with st.spinner('Processing data...'):

    df_predictions = pd.merge(df_predictions, df_sun, how = 'left', on= 'Icon_code')

    df_provincias = cod_provincias(df_provincias)

    df_predictions = process_df_predictions(df_predictions, df_provincias)

    df_embalses = embalses_latest_data(df_embalses, df_provincias)

    df_power_installed = powerinstalled_CCAA_process(df_power_installed)
    df_power_installed = pivot_table_generation_by_ccaa(df_power_installed)
    df_power_installed= power_installed_last_month(df_power_installed)
    df_power_installed = REE_ccaa_rename(df_power_installed)
    #We can drop 'fecha' already
    df_power_installed = df_power_installed.drop(columns = ['fecha'])

    #Let's mix it to have the full df:

    df_predictions_total = all_predictions(df_predictions, df_embalses, df_power_installed)
    df_predictions_total = fill_na_predictions(df_predictions_total)

st.success('Data processed')

tab1,tab2 = st.tabs(["Analysis", "Data"])

with tab2:

    st.write(df_predictions_total)

with tab1:

    #Let's scale values before making the prediction, with the best scalator we have tested for our data:

    df_predictions_total_scaled = standarization_minmax_scaler(df_predictions_total)

    #now we can filter to obtain several dataframe, one ofr each target

    pred_solartermica, pred_solarfotovoltaica, pred_hidraulica, pred_eolica, pred_demand = filter_predictions_df(df_predictions_total_scaled)

    #Create a new dataframe that will contain all the predictions to see the completion of the demand:

    df_demand_completion = pd.DataFrame({'fecha': df_predictions_total_scaled['fecha'].unique()})

    

    col1, col2, col3, col4, col5 = st.columns(5, gap = 'small')

    with col1:

        st.markdown(""" #### Solar Term Gener. expected""" )

        data = pred_solartermica
        data = apply_model(data, model_solarterm)

        data = data.groupby('fecha')['prediction'].sum().reset_index()

        df_demand_completion['solar_term'] = data['prediction']

        fig = plot_bars_predictions(data)

        st.pyplot(fig)

    with col2:

        st.markdown(""" #### Solar Foto Gener. expected""" )

        data = pred_solarfotovoltaica

        data = apply_model(data, model_solarfoto)

        data = data.groupby('fecha')['prediction'].sum().reset_index()

        df_demand_completion['solar_foto'] = data['prediction']

        fig = plot_bars_predictions(data)
        st.pyplot(fig)
    

    with col3:

        st.markdown(""" #### Hidraulic Gener. expected""" )

        data = pred_hidraulica

        data = apply_model(data, model_hidro)

        data = data.groupby('fecha')['prediction'].sum().reset_index()

        df_demand_completion['Hidro'] = data['prediction']

        fig = plot_bars_predictions(data)
        st.pyplot(fig)

    with col4:

        st.markdown(""" #### Eolic Gener.expected""" )

        data = pred_eolica

        model = download_model(url_eolic)

        data = apply_model(data, model_eolic)

        data = data.groupby('fecha')['prediction'].sum().reset_index()

        df_demand_completion['eolic'] = data['prediction']

        fig = plot_bars_predictions(data)
        st.pyplot(fig)

    with col5:

        st.markdown(""" #### Demand expected""" )

        data = pred_demand

        data = apply_model(data, model_demand)

        data = data.groupby('fecha')['prediction'].sum().reset_index()

        df_demand_completion['demand'] = data['prediction']

        fig = plot_bars_predictions(data)
        st.pyplot(fig)


df_demand_completion['Total_rw_generation'] = df_demand_completion['solar_term']+df_demand_completion['solar_foto']+df_demand_completion['Hidro']+df_demand_completion['eolic']
df_demand_completion['demand_covered_percent'] = df_demand_completion['Total_rw_generation'] / df_demand_completion['demand']
df_demand_completion['fecha'] = df_demand_completion['fecha'].astype(str)
df_demand_completion['fecha'] = df_demand_completion['fecha'].str[:10]

st.header("Demand covered with renewable energy by date", divider = 'grey')

col1, col2, col3, col4, col5, col6, col7 = st.columns(7, gap = 'small')


with col1:
    valuecompletion = np.round(df_demand_completion['demand_covered_percent'].iloc[0]*100,1)
    datepercent = df_demand_completion['fecha'].iloc[0]
    valuepending = np.round(100 - valuecompletion,1)
    values = [valuepending, valuecompletion]
    st.subheader(f"{datepercent}", divider = 'grey')
    fig = plot_completion(values)
    st.pyplot(fig)

with col2:
    valuecompletion = np.round(df_demand_completion['demand_covered_percent'].iloc[1]*100,1)
    datepercent = df_demand_completion['fecha'].iloc[1]
    valuepending = np.round(100 - valuecompletion,1)
    values = [valuepending, valuecompletion]
    st.subheader(f"{datepercent}", divider = 'grey')
    fig = plot_completion(values)
    st.pyplot(fig)

with col3:
    valuecompletion = np.round(df_demand_completion['demand_covered_percent'].iloc[2]*100,1)
    datepercent = df_demand_completion['fecha'].iloc[2]
    valuepending = np.round(100 - valuecompletion,1)
    values = [valuepending, valuecompletion]
    st.subheader(f"{datepercent}", divider = 'grey')
    fig = plot_completion(values)
    st.pyplot(fig)

with col4:
    valuecompletion = np.round(df_demand_completion['demand_covered_percent'].iloc[3]*100,1)
    datepercent = df_demand_completion['fecha'].iloc[3]
    valuepending = np.round(100 - valuecompletion,1)
    values = [valuepending, valuecompletion]
    st.subheader(f"{datepercent}", divider = 'grey')
    fig = plot_completion(values)
    st.pyplot(fig)

with col5:
    valuecompletion = np.round(df_demand_completion['demand_covered_percent'].iloc[4]*100,1)
    datepercent = df_demand_completion['fecha'].iloc[4]
    valuepending = np.round(100 - valuecompletion,1)
    values = [valuepending, valuecompletion]
    st.subheader(f"{datepercent}", divider = 'grey')
    fig = plot_completion(values)
    st.pyplot(fig)

with col6:
    valuecompletion = np.round(df_demand_completion['demand_covered_percent'].iloc[5]*100,1)
    datepercent = df_demand_completion['fecha'].iloc[5]
    valuepending = np.round(100 - valuecompletion,1)
    values = [valuepending, valuecompletion]
    st.subheader(f"{datepercent}", divider = 'grey')
    fig = plot_completion(values)
    st.pyplot(fig)

with col7:
    valuecompletion = np.round(df_demand_completion['demand_covered_percent'].iloc[6]*100,1)
    datepercent = df_demand_completion['fecha'].iloc[6]
    valuepending = np.round(100 - valuecompletion,1)
    values = [valuepending, valuecompletion]
    st.subheader(f"{datepercent}", divider = 'grey')
    fig = plot_completion(values)
    st.pyplot(fig)


