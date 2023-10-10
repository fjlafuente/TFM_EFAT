import pandas as pd
import numpy as np
import requests as requests
import json
from requests.auth import HTTPBasicAuth
import urllib
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from pandas import to_datetime
import seaborn as sns
import matplotlib.pyplot as plt
from unidecode import unidecode
import pandas as pd
import requests as requests
import json
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
import statistics
import time
import tempfile
import geopandas as gpd
from shapely.affinity import translate
from io import BytesIO
import zipfile
import tempfile
import os
import shutil
from io import StringIO
import gdown
from matplotlib.colors import LinearSegmentedColormap, Normalize
from joblib import load
from sklearn.preprocessing import OneHotEncoder
from PIL import Image
from io import BytesIO
from io import StringIO
import io
import subprocess
import shlex

























def data_REE_generation(year): # -> REE API Only allows to extract data in a yearly basis

    #First we get the response from REE (It only allow us to see one year each time)

    response = requests.get(f"https://apidatos.ree.es/es/datos/generacion/estructura-generacion?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day")
    
    #Data comes in a json with dictionaries inside. To access to the data we have to proccess it a little bit with json and dictionary methods.

    generation = response.json()['included']
    generation = pd.DataFrame.from_dict(generation)['attributes']
    generation = pd.json_normalize(generation)
    generation = generation[['title','values']]
    
    #We create a for loop in order to access to all data in the dicionary and to get the complete list for all the energetic resources
    
    typelist = list(generation['title'])
    n= 0
    data_consolidated = pd.DataFrame(columns= ['value', 'percentage', 'datetime', 'Type'])
    
    for value in generation['values']:
 
        data = pd.DataFrame.from_dict(value)
        data['Type'] = typelist[n]
        n += 1
        data_consolidated = pd.concat([data_consolidated, data])
        
    return data_consolidated


def data_REE_demand(year): # -> REE API Only allows to extract data in a yearly basis

     #First we get the response from REE (It only allow us to see one year each time)

    demand = requests.get(f"https://apidatos.ree.es/es/datos/demanda/evolucion?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day")

    #Data comes in a json with dictionaries inside. To access to the data we have to proccess it a little bit with json and dictionary methods.

    demand = demand.json()['included']
    demand = pd.DataFrame.from_dict(demand)['attributes']
    demand = pd.json_normalize(demand)
    demand = demand['values']

    #We create a for loop in order to access to all data in the dicionary and to get the complete list for all the energetic resources

    n= 0
    data_consolidated = pd.DataFrame(columns= ['value', 'percentage', 'datetime'])
    
    for value in demand:
 
        data = pd.DataFrame.from_dict(value)
        n += 1
        data_consolidated = pd.concat([data_consolidated, data])
    return data_consolidated

def data_REE_potencia_instalada(yearini, yearend): 

    """ 
    REE API Only allows to extract data in a yearly range as much, in monthly basis. We are downloading the info through a loop, selecting the desired fields in the json file
    """

    years = range(yearini,yearend + 1)
    all_data = []

    #First we get the response from REE (It only allow us to see one year each time). We will create an empty list that will be appended

    for retry in range(3):
        try:

            for year in years:

                #First we get the response from REE (It only allow us to see one year each time)

                response = requests.get(f"https://apidatos.ree.es/es/datos/generacion/potencia-instalada?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day&all_ccaa=allCcaa")
                
                #Data comes in a json with dictionaries inside. To access to the data we have to proccess it a little bit with json and dictionary methods.

                pinstalled = response.json()['included']

                for ccaa in pinstalled:

                    cc_aa = ccaa['community_name']
                    
                    #We create a for loop in order to access to all data in the dicionary and to get the complete list for all the energetic resources
                    
                    for item in ccaa['content']:

                        techonology = item['type']
                
                        for element in item['attributes']['values']:
                            
                            value = element['value']
                            month = element['datetime']
                            all_data.append({
                                'comunidad_autonoma': cc_aa,
                                'type': techonology,
                                'month': month,
                                'value': value
                            })
                    time.sleep(3.0)
            data_consolidated = pd.DataFrame(all_data)
        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry
    
    return data_consolidated


def power_installed_last_month(powerinstalled_file):
    lastmonth = powerinstalled_file['fecha'].max()
    power_file = powerinstalled_file[powerinstalled_file['fecha'] == lastmonth]
    power_file = power_file.rename(columns= {'Eólica': 'inst_Eólica',
                                                       'Hidráulica': 'inst_Hidráulica',
                                                       'Solar fotovoltaica': 'inst_Solar fotovoltaica',
                                                       'Solar térmica': 'inst_Solar_térmica'
                                                       })

    return power_file

def data_REE_generation_by_ccaa(year): #-> In order to know generation per CCAA per month

    response = requests.get(f"https://apidatos.ree.es/es/datos/generacion/estructura-generacion?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day&all_ccaa=allCcaa")
    
    #To convert response into a pd.DataFrame

    generation_ccaa = response.json()['included']
    generation_ccaa = pd.json_normalize(generation_ccaa)

    #In order to include afterwards the reference for the CCAA when processing the file (it has dictionaries, lists... the proccess is complicated)
    
    ccaa_info = generation_ccaa[['geo_id', 'community_name']]
    ccaa_info['ccaa'] = range(0,20)

    #We access to 'content' key, where the information we want is, and create an empty dataframe to be fullfilled by iterating in the items

    content = generation_ccaa['content'].to_dict()
    total_data = pd.DataFrame(columns = ['ccaa', 'month', 'type', 'value', 'datetime', 'percentage'])

    #Iteration will be based on keylist due to the response structure:

    keylist = list(content.keys())
    
    for item in keylist:
        data = content[item]
        ccaa = item
        for element in data:
            data_selected = element['attributes']
            data_selected = data_selected['values']
            tech = element['type']
            n = 0
            for month in data_selected:
                    n += 1
                    df = pd.DataFrame(month, columns = ['value', 'percentage', 'datetime'], index = [n])
                    df['type'] = tech
                    df['ccaa'] = ccaa
                    df['month'] = n
                    total_data = pd.concat([total_data, df])

    total_data_info = pd.merge(total_data , ccaa_info, on='ccaa', how = 'inner')

    #We will already drop the information of 'total cca' as it won't be necessary, we already have it

    data_filt = total_data_info.loc[total_data_info['ccaa'] != 19]

    return data_filt




def generation_by_CCAA_csv_file(year): # -> So we can save all years data in our project's directory
    generation = data_REE_generation_by_ccaa(year)
    generation.to_csv(f"~/data/TFM_EFAT/TFM_EFAT/Data/Generation/Generation_by_CCAA/Generation_ccaa_{year}.csv", index = False)


def demand_csv_file(year): # -> So we can save all years data in our project's directory
    demand = data_REE_demand(year)
    demand.to_csv(f"~/data/TFM_EFAT/TFM_EFAT/Data/Demand/Demand_{year}.csv", index = False)

def pinstalled_csv_file(year): # -> So we can save all years data in our project's directory
    pinstalled = data_REE_potencia_instalada(year)
    pinstalled.to_csv(f"~/data/TFM_EFAT/TFM_EFAT/Data/Generation/PowerInstalled_{year}.csv", index = False)

def generation_csv_file(year): # -> So we can save all years data in our project's directory
    generation = data_REE_generation(year)
    generation.to_csv(f"~/data/TFM_EFAT/TFM_EFAT/Data/Generation/Generation_{year}.csv", index = False)    

def aemet_data_api(year): # -> in order to get weather records from AEMET API

    #It only allows to extract data in a mothly basis, so we have to create a for loop to solve it:

    monthlist = ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12')

    #To see if year is a leap year:

    feb = 'feb'

    if year % 100 == 0:
        if year % 400 == 0:
            feb = '29'
        else:
            feb = '28'
    else:
        if year  % 4 == 0:
            feb = '29'
        else:
            feb = '28'

    monthlastday = {'01': '31', '02':feb, '03': '31', '04':'30', '05': '31', '06':'30', '07': '31', 
                    '08': '31', '09':'30', '10': '31', '11':'30', '12': '31'}

    aemet_consolidated = pd.DataFrame(columns = ['fecha', 'indicativo', 'nombre', 'provincia', 'altitud', 'tmed', 'prec', 'tmin', 'horatmin', 'tmax',
                                    'horatmax', 'dir', 'velmedia', 'racha', 'horaracha', 'sol', 'presMax', 'horaPresMax', 'presMin', 'horaPresMin'])


    for month in monthlist:

        fechaIniStr = f"{year}-{month}-01T00:00:00UTC" # str | Fecha Inicial (AAAA-MM-DDTHH:MM:SSUTC)
        fechaFinStr = f"{year}-{month}-{monthlastday[month]}T23:59:59UTC"  # str | Fecha Final (AAAA-MM-DDTHH:MM:SSUTC)

        url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{fechaFinStr}/todasestaciones"

        #We need an API key that can be obtained from AEMET easily
        
        query = {"api_key":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqYXZpZXIuZXNjYWxvbmlsbGFAaG90bWFpbC5jb20iLCJqdGkiOiJlNzgyMjg0Yy05YjI0LTQ5ZDktOWMwMS1kYjRlZjQwNjkxNDIiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTY4MTE0MTIyNCwidXNlcklkIjoiZTc4MjI4NGMtOWIyNC00OWQ5LTljMDEtZGI0ZWY0MDY5MTQyIiwicm9sZSI6IiJ9.3flzKWh31FkeRBFex1xc4nIwEaQE1QPXoCpeicIluQU"}

        #We have to process the response from requests a little bit as the response is a url

        response = requests.request("GET", url,  params = query)

        aemet_data = response.json()['datos']

        aemet_data = urllib.request.urlopen(aemet_data)

        # UTF-8 decoding, which is the standard, does not work with some characters of the response

        aemet_data = json.loads(aemet_data.read().decode('latin-1'))
        
        aemet_data_df = pd.DataFrame.from_dict(aemet_data)

        aemet_consolidated = pd.concat([aemet_consolidated, aemet_data_df])
    
    return aemet_consolidated


def download_embalses():
    """
    In order to download and update our 'Embalses' info file which will contain the basic data for hydro estimations.
    Download the zipfile in the current directory, extract the information, and copy it into a variable, then delete the zipfile.
    """

    for retry in range(3):
        try:
            # The file is stored on the 'Ministerio para la Transición Ecológica y reto demográfico' webpage, and it is stored in zip format
            embalses = requests.get('https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico/Historico-de-embalses/BD-Embalses.zip')

            if embalses.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(embalses.content), 'r') as zip_embalses:
                    zip_embalses.extractall()

                # MDB file path
                mdb_file = 'BD-Embalses.mdb'

                # Table name. It has to be adjusted within the years
                table_name = "T_Datos Embalses 1988-2023"

                # Execute the mdb-export command and get the output as a variable
                command = f"mdb-export {mdb_file} \"{table_name}\""
                output = subprocess.run(shlex.split(command), capture_output=True, text=True, encoding='latin-1').stdout

                # Convert the output into a Pandas DataFrame using the correct encoding
                embalses_data = pd.read_csv(io.StringIO(output), encoding='latin-1')

                # Remove files
                os.remove(mdb_file)

                break
            else:
                time.sleep(5.0)

        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry  

    return embalses_data

def aemet_municipios(): # -> in order to get masterdata from AEMET API

    #AEMET API does not allow to extract predictions for all Municipios all the same time, therefore we firstly have to download the masterdata:

    url = f"https://opendata.aemet.es/opendata/api/maestro/municipios"

    #We need an API key that can be obtained from AEMET easily
    
    query = {"api_key":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqYXZpZXIuZXNjYWxvbmlsbGFAaG90bWFpbC5jb20iLCJqdGkiOiJlNzgyMjg0Yy05YjI0LTQ5ZDktOWMwMS1kYjRlZjQwNjkxNDIiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTY4MTE0MTIyNCwidXNlcklkIjoiZTc4MjI4NGMtOWIyNC00OWQ5LTljMDEtZGI0ZWY0MDY5MTQyIiwicm9sZSI6IiJ9.3flzKWh31FkeRBFex1xc4nIwEaQE1QPXoCpeicIluQU"}

    #We create an empty dataframe with the columns that will be appended

    aemet_municipios_total = pd.DataFrame(columns = ['latitud', 'id_old', 'url', 'latitud_dec', 'altitud', 'capital',
        'num_hab', 'zona_comarcal', 'destacada', 'nombre', 'longitud_dec', 'id',
        'longitud'])
    
    for retry in range(3):
        try:
    
            response = requests.request("GET", url,  params = query)

            if response.status_code == 200:

                aemet_municipios = response.json()

                #The response is a list of dicts, in which each dict is one municipio:

                for item in aemet_municipios:

                    municipio = pd.DataFrame(item, index= [0])
                    aemet_municipios_total = pd.concat([aemet_municipios_total, municipio])
                aemet_municipios_total.reset_index()

                return aemet_municipios_total
        
            else:
                print("Server error. Trying again...")
                time.sleep(5.0)  # Wait for another retry
        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry
    return None


def aemet_municipios_predictions(municipios): # -> in order to get predictions for next seven days from AEMET API

    #We are going to extract the predictions of a sample of municipios for each province in Spain

    municipios_id = list(municipios['id'])

    #Now we create a loop in order to access to all the required information for each municipio:

    for retry in range(3):

        #We create a dataframe that will be appended based on the information provided by AEMET

        df_prediction = pd.DataFrame(columns= ['id_municipio', 'nombre', 'provincia', 'fecha', 'tmax', 'tmin', 'Icon_code', 'viento', 'racha'])

        try:

            for municipio in municipios_id:
                
                #In order to avoid blocking AEMET servers:

                time.sleep(0.5)

                #We only need municipio ID:

                municipio = municipio[2:]

                url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/{municipio}"

                #We need an API key that can be obtained from AEMET easily

                query = {"api_key":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqYXZpZXIuZXNjYWxvbmlsbGFAaG90bWFpbC5jb20iLCJqdGkiOiJlNzgyMjg0Yy05YjI0LTQ5ZDktOWMwMS1kYjRlZjQwNjkxNDIiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTY4MTE0MTIyNCwidXNlcklkIjoiZTc4MjI4NGMtOWIyNC00OWQ5LTljMDEtZGI0ZWY0MDY5MTQyIiwicm9sZSI6IiJ9.3flzKWh31FkeRBFex1xc4nIwEaQE1QPXoCpeicIluQU"}

                response = requests.request("GET", url,  params = query, timeout = 500)

                
                if response.status_code != 200:

                    #We have another API key if the main one fails. We repeat the same procedure:

                    time.sleep(0.5)

                    query = {"api_key": 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJsYWZ1bGFmdWVudGVAZ21haWwuY29tIiwianRpIjoiNGZlY2Y3MzMtYTY3Yy00M2ZmLTgxODMtZTM1N2Q0ODc0YzM5IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE2ODg1MTM0MjksInVzZXJJZCI6IjRmZWNmNzMzLWE2N2MtNDNmZi04MTgzLWUzNTdkNDg3NGMzOSIsInJvbGUiOiIifQ.WCoq7p6RyV6thyXkkzUmgg27jt2AsJBK3uVTrv2uBcI'}

                    response = requests.request("GET", url,  params = query, timeout = 500)

                    if response.status_code != 200:
                        raise ValueError('API Request not succesfull')

                    predictions = response.json()['datos']

                    predictions = urllib.request.urlopen(predictions)
                    predictions = json.loads(predictions.read().decode('latin-1'))

                    #As usual, json file provides the information in many dictionaries, lists etc. and it is not easy to process it:
                
                    for item in predictions:
                        item_id = item['id']
                        nombre = item['nombre']
                        provincia = item['provincia']
                        prediccion = item['prediccion']
                        prediccion = prediccion['dia']
                        for day in prediccion:
                            fecha = day['fecha']

                            #For get the temperature(max & min):

                            temp = day['temperatura']
                            tmax = temp['maxima']
                            tmin = temp['minima']

                            #In order to access to isolation information. We will map the information afterwards

                            estado_del_cielo = []
                            for data in day['estadoCielo']:

                                if data['value'] == '':
                                    pass
                                else:
                                    estado_id = data['value']
                                    estado_del_cielo.append(estado_id)

                            #Wind information

                            velocidad_viento = []
                            for data in day['viento']:
                                
                                velocidad = float(data['velocidad'])
                                velocidad_viento.append(velocidad)

                            racha_viento = []
                            for data in day['rachaMax']:

                                if data['value'] == '':
                                    pass
                                else:
                                    rachamax = float(data['value'])
                                    racha_viento.append(rachamax)
                            
                            #Now the dataframe is created:

                            estado_cielo = statistics.mode(estado_del_cielo)
                            viento = statistics.mean(velocidad_viento)
                            if len(racha_viento) == 0: #In order to avoid making the mean with '0' values that are errors:
                                racha = None
                            else:
                                racha = statistics.mean(racha_viento)
                                
                            datadict = {'id_municipio': item_id, 'nombre': nombre, 'provincia':provincia, 'fecha': fecha, 'tmax': tmax, 'tmin':tmin, 'Icon_code' : estado_cielo, 'viento': viento, 'racha' :racha}
                            total_prediction = pd.DataFrame(datadict, index = [0])
                            df_prediction = pd.concat([df_prediction, total_prediction])
                    
                    
                        

                else:
                    predictions = response.json()['datos']
                    
                    predictions = urllib.request.urlopen(predictions)
                    predictions = json.loads(predictions.read().decode('latin-1'))

                    #As usual, json file provides the information in many dictionaries, lists etc. and it is not easy to process it:
                
                    for item in predictions:
                        item_id = item['id']
                        nombre = item['nombre']
                        provincia = item['provincia']
                        prediccion = item['prediccion']
                        prediccion = prediccion['dia']
                        for day in prediccion:
                            fecha = day['fecha']

                            #For get the temperature(max & min):

                            temp = day['temperatura']
                            tmax = temp['maxima']
                            tmin = temp['minima']

                            #In order to access to isolation information. We will map the information afterwards

                            estado_del_cielo = []
                            for data in day['estadoCielo']:

                                if data['value'] == '':
                                    pass
                                else:
                                    estado_id = data['value']
                                    estado_del_cielo.append(estado_id)

                            #Wind information

                            velocidad_viento = []
                            for data in day['viento']:
                                
                                velocidad = float(data['velocidad'])
                                velocidad_viento.append(velocidad)

                            racha_viento = []
                            for data in day['rachaMax']:

                                if data['value'] == '':
                                    pass
                                else:
                                    rachamax = float(data['value'])
                                    racha_viento.append(rachamax)
                            
                            #Now the dataframe is created:

                            estado_cielo = statistics.mode(estado_del_cielo)
                            viento = statistics.mean(velocidad_viento)
                            if len(racha_viento) == 0: #In order to avoid making the mean with '0' values
                                racha = None
                            else:
                                racha = statistics.mean(racha_viento)
                            datadict = {'id_municipio': item_id, 'nombre': nombre, 'provincia':provincia, 'fecha': fecha, 'tmax': tmax, 'tmin':tmin, 'Icon_code' : estado_cielo, 'viento': viento, 'racha' :racha}
                            total_prediction = pd.DataFrame(datadict, index = [0])
                            df_prediction = pd.concat([df_prediction, total_prediction])

            return df_prediction
                        
        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry           


    return df_prediction

def weather_csv_file(year): # -> So we can save all years data in our project's directory
    weather = aemet_data_api(year)
    weather.to_csv(f"~/data/TFM_EFAT/TFM_EFAT/Data/Weather/Weather{year}.csv", index = False)




## PROCESSING

def weather_processing(weather_file):

    """
    Process weather_file:
    - Correctes some values in 'prec' that contain text when 'prec' is so low.
    - Converts into numeric the vairables that we are interested in.
    - Selects and group those variables by 'fecha'  and 'provincia'.
    """

    weather_file_processed = weather_file

    #Date to datetime:

    weather_file_processed['fecha'] = weather_file_processed['fecha'].apply(to_datetime)

    #In order to replace 'Ip' values in 'prec':

    weather_file_processed.loc[weather_file_processed['prec'] == 'Ip', 'prec'] = 0
    
   #Let's transform columns into numeric: 

    columns_to_transform = ['prec', 'velmedia', 'racha', 'tmed', 'tmin', 'tmax', 'sol']
    
    for column in columns_to_transform:
        weather_file_processed[column] = weather_file_processed[column].str.replace(',', '.')
        weather_file_processed[column] = weather_file_processed[column].apply(pd.to_numeric, errors = 'coerce')



    #Our analysis will be based on 'Provincia' info, therefore we need to have the information grouped by that column. We can simplify tis way also the dataframe:

    weather_file_processed = weather_file_processed.groupby(['fecha', 'provincia'], as_index=False).agg(
        {'tmed': 'mean',
         'prec': 'sum',
         'tmin': 'mean',
         'tmax': 'mean',
         'dir': 'mean',
         'velmedia': 'mean',
         'racha': 'mean',
         'sol': 'mean',}).round(3)

    #It is necessary also to already adjust some 'Provincia' names in order to avoid errors when matching the files afteerwards:

    weather_file_processed['provincia'] = weather_file_processed['provincia'].replace('VALENCIA','VALENCIA/VALENCIA')
    weather_file_processed['provincia'] = weather_file_processed['provincia'].replace('A CORUÑA','A CORUNA')
    weather_file_processed['provincia'] = weather_file_processed['provincia'].replace('ALICANTE','ALICANTE/ALACANT')
    weather_file_processed['provincia'] = weather_file_processed['provincia'].replace('CASTELLON','CASTELLON/CASTELLO')
    weather_file_processed['provincia'] = weather_file_processed['provincia'].replace('STA. CRUZ DE TENERIFE','SANTA CRUZ DE TENERIFE')

    #We exlude 2014 from the study:

    weather_file_processed = weather_file_processed[weather_file_processed['fecha'].dt.year != 2014]

    return weather_file_processed

def constrain_weather (weather_file): 

    #In order to simplify the data, eliminating the geo info by making the mean per day
    
    weather_pivot_table = weather_file[['fecha', 'provincia', 'tmed', 'prec', 'tmin', 'tmax', 'velmedia', 'racha', 'sol']]
    weather_pivot_table = weather_file.groupby(['fecha', 'provincia'], as_index=False)[['tmed', 'prec', 'tmin', 'tmax', 'velmedia', 'racha', 'sol']].mean()
    weather_pivot_table = weather_pivot_table.groupby('fecha', as_index = False)[['tmed', 'prec', 'tmin', 'tmax', 'velmedia', 'racha', 'sol']].mean()

    return weather_pivot_table


def embalses_select_year (df, presas_file, year):

    """
    Mix the 'embalses' file with masterdata to obtain the province.
    Select only the year mentioned and the dams with hydroelectric generation.
    """

    #Firstly, we read the 'embalses' data and select only the year we want and to process 'Embalse Nombre' in order to match it with 'presas' file.
    #We only want the dams with the electricty flag marked
    
    embalses_elect = df
    embalses_elect = embalses_elect.loc[embalses_elect['ELECTRICO_FLAG'] == 1]
    embalses_elect['year'] = pd.DatetimeIndex(embalses_elect['FECHA']).year

    #In order to transform water KPIs into numeric
    str(embalses_elect['AGUA_TOTAL'])
    embalses_elect['AGUA_TOTAL'] = embalses_elect['AGUA_TOTAL'].str.replace(',' , '.')
    embalses_elect['AGUA_TOTAL'] = embalses_elect['AGUA_TOTAL'].apply(pd.to_numeric)
    str(embalses_elect['AGUA_ACTUAL'])
    embalses_elect['AGUA_ACTUAL'] = embalses_elect['AGUA_ACTUAL'].str.replace(',' , '.')
    embalses_elect['AGUA_ACTUAL'] = embalses_elect['AGUA_ACTUAL'].apply(pd.to_numeric)

    #In order to have the information of the year that we want and to get the name of the dam in the way to match it with 'presas' file

    embalses_elect = embalses_elect.query('year == @year')
    embalses_elect['EMBALSE_NOMBRE'] = embalses_elect['EMBALSE_NOMBRE'].apply(str.upper).apply(unidecode)

    #Now, we have to preprocess 'presas' data

    presas = presas_file
    presas.rename({'Presa':'EMBALSE_NOMBRE'}, axis = 1, inplace = True)
    presas['EMBALSE_NOMBRE'] = presas['EMBALSE_NOMBRE'].apply(unidecode)

    #At last, the joint. It will be some dams not matched but are not relevant for our study:

    embalses_capacity = embalses_elect.merge(presas, on = 'EMBALSE_NOMBRE', how= 'inner')
    
    return embalses_capacity

def codificar_sol(x):

    #In order to align both files: the used for training and the new data with the same category

    if x <= 2:
        return 0
    if 2 < x <= 4:
        return 1
    if 4 < x <= 6:
        return 2
    if 6 < x <= 8:
        return 3
    if 8 < x <= 10:
        return 4
    if 10 < x:
        return 5
    
def filter_consolidated_df(consolidated_file):

    ccaa_solartermica = consolidated_file[consolidated_file['Solar térmica'] != 0]['comunidad_autonoma'].unique()
    ccaa_solarfotovoltaica = consolidated_file[consolidated_file['Solar fotovoltaica'] != 0]['comunidad_autonoma'].unique()
    ccaa_hidraulica = consolidated_file[consolidated_file['Eólica'] != 0]['comunidad_autonoma'].unique()
    ccaa_eolica = consolidated_file[consolidated_file['Hidráulica'] != 0]['comunidad_autonoma'].unique()

    #Based on the information provided by the Pndas Profile and the other notebooks, we can drop alredy some variables that we know are not relevant for each technology:
    
    df_solartermica = consolidated_file[consolidated_file['comunidad_autonoma'].isin(ccaa_solartermica)]
    df_solartermica = df_solartermica[['fecha', 'comunidad_autonoma','tmed','tmax','velmedia','sol','Solar térmica', 'inst_Solar_térmica']] \
        .rename(columns = {'Solar térmica':'generation'})
    df_solarfotovoltaica = consolidated_file[consolidated_file['comunidad_autonoma'].isin(ccaa_solarfotovoltaica)]
    df_solarfotovoltaica = df_solarfotovoltaica[['fecha', 'comunidad_autonoma','tmed','tmax','velmedia','sol','Solar fotovoltaica', 'inst_Solar fotovoltaica']] \
        .rename(columns = {'Solar fotovoltaica':'generation'})
    df_hidraulica = consolidated_file[consolidated_file['comunidad_autonoma'].isin(ccaa_hidraulica)]
    df_hidraulica = df_hidraulica[['fecha','comunidad_autonoma','tmed','prec','AGUA_ACTUAL','AGUA_TOTAL', 'Hidráulica', 'inst_Hidráulica']] \
        .rename(columns = {'Hidráulica':'generation'})
    df_eolica = consolidated_file[consolidated_file['comunidad_autonoma'].isin(ccaa_eolica)]
    df_eolica = df_eolica[['fecha', 'comunidad_autonoma','tmed', 'prec', 'velmedia', 'racha', 'Eólica', 'inst_Eólica']] \
        .rename(columns = {'Eólica':'generation'})
    df_demand = consolidated_file[['fecha', 'comunidad_autonoma','tmed', 'sol', 'Weekday', 'prec', 'demand_ccaa']]

    return df_solartermica, df_solarfotovoltaica, df_hidraulica, df_eolica, df_demand

def create_spain_map(spain_file):
    #To access to the geo data we are using geopandas library, which transforms data into readable dataframes:
    spain_file = spain_file.rename(columns = {'NAME_1': 'comunidad_autonoma', 'CC_1':'CODAUTO'})
    spain_file['comunidad_autonoma'] = (spain_file['comunidad_autonoma'].apply(unidecode, 'utf-8')).str.upper()
    spain_file['comunidad_autonoma'] = spain_file['comunidad_autonoma'].replace({'CEUTA Y MELILLA':'MELILLA',
                                                                    'COMUNIDAD VALENCIANA': 'COMUNITAT VALENCIANA',
                                                                    'ISLAS BALEARES': 'ILLES BALEARS',
                                                                    'ISLAS CANARIAS': 'CANARIAS'})
    
    #We don't need all the information from the file:

    spain_file = spain_file[['comunidad_autonoma', 'geometry']]

    return spain_file

def translate_canarias(spain_file):
    
    # Apply translate to 'geometry' column based on condition
    spain_file['geometry'] = spain_file.apply(
    lambda row: translate(row['geometry'], xoff=3, yoff=6) if row['comunidad_autonoma'] == 'CANARIAS' else row['geometry'],
    axis=1
    )

    return spain_file

def process_df_predictions(prediction_df, codprovincias_file):

        """ 
        Rename name of some provinces
        Extract the tmed feature as a mean of tmax & tmin
        Merging prediction dataframe & codprovincias file to obtain the CCAA
        Groupping by the dataframe to obtain the mean by CCAA and date
        """
        #We need some replacements in 'provincias' in order to match it with the other files:

        prediction_df['provincia'] = prediction_df['provincia'].replace('València/Valencia','Valencia/València')
        prediction_df['provincia'] = prediction_df['provincia'].replace('Illes Balears (Mallorca)','Illes Balears')
        prediction_df['provincia'] = prediction_df['provincia'].replace('Las Palmas (Gran Canaria)','Las Palmas')
        prediction_df['provincia'] = prediction_df['provincia'].replace('Santa Cruz de Tenerife (Tenerife)','Santa Cruz de Tenerife')
        prediction_df['provincia'] = prediction_df['provincia'].replace('Alacant/Alicante','Alicante/Alacant')
        prediction_df['provincia'] = prediction_df['provincia'].replace('Castelló/Castellón','Castellón/Castelló')

        prediction_df['provincia'] = prediction_df['provincia'].apply(unidecode).str.upper()

        #We are creating also the 'tmed' categoory so we can cross it with training data:

        prediction_df['tmed'] = (prediction_df['tmax'] + prediction_df['tmin']) / 2

        #Let's rename also 'viento' to 'velmedia' and 'cod_sol' to 'sol' so we can cross it:

        prediction_df = prediction_df.rename(columns= {'viento': 'velmedia',
                                                       'cod_sol': 'sol'})
        
        #Icon_code can be dropped:

        prediction_df = prediction_df.drop(columns = 'Icon_code')

        #We cross it with provincias file:

        prediction_df = pd.merge(prediction_df, codprovincias_file, how = 'left', on= 'provincia')

        prediction_df = prediction_df.groupby(['fecha', 'comunidad_autonoma'],as_index=False).agg(
        {'tmed': 'mean',
         'tmin': 'mean',
         'tmax': 'mean',
         'velmedia': 'mean',
         'racha': 'mean',
         'sol': 'mean',})
        
        cols_to_convert = ['tmed', 'tmin', 'tmax', 'velmedia', 'racha', 'sol']
        prediction_df[cols_to_convert] = prediction_df[cols_to_convert].astype('float64').round(3)

        return prediction_df


def embalses_latest_data(embalses_file, cod_provincias_file):

    """ 
    In order to prepare the information to insert it into the trained model.
    Selects latest information available and cross it with provinces file to obtain the CCAA.
    """

    #We only need information accumulated by 'Provincia' and 'FECHA':

    embalses_file = (
        embalses_file.groupby(
        by =['FECHA','Provincia'], as_index =False)[['AGUA_TOTAL', 'AGUA_ACTUAL']]
        .sum()
    )
    #For the new data analysis we will use always the ,latest information available on the file:
    embalses_last_date = embalses_file['FECHA'].max()
    embalses_file = embalses_file[embalses_file['FECHA'] == embalses_last_date].reset_index(drop = True)

    #We need to modify some 'Provincia' names:
    embalses_file['Provincia'] = embalses_file['Provincia'].replace('Coruña, A' ,'A Coruña')

    #We drop now 'FECHA' column as it is not necessary anymore
    embalses_file = embalses_file.drop(columns = ['FECHA'])

    #In order to have'Provincias' in the same form as the other files:

    embalses_file = embalses_file.rename(columns = {'Provincia': 'provincia'})
    embalses_file['provincia'] = embalses_file['provincia'].apply(unidecode).str.upper()

    #We have to merge it with codprovincias to get the 'comunidad_autonom' columns
    embalses_file = pd.merge(embalses_file, cod_provincias_file, on = 'provincia', how = 'left')

    #Groupping by CCAA so we have the info the same way as the other files:
    embalses_file = embalses_file.groupby('comunidad_autonoma', as_index = False)[['AGUA_TOTAL','AGUA_ACTUAL']].sum().round(3)

    return embalses_file


def filter_predictions_df(predictions_dataframe):

    """ 
    Creates from a single consolidated dataframe 5 dataframes, one for each target.
    Filter the 'comunidades autonomas' relevant for each category.
    Selects for each dataframe the relevant variables to consider
    Makes the OneHotEncoder for each dataframe
    """

    ccaa_solartermica = ['ANDALUCIA', 'CASTILLA-LA MANCHA', 'CATALUNA', 'COMUNITAT VALENCIANA','EXTREMADURA','REGION DE MURCIA']
    ccaa_solarfotovoltaica = ['ANDALUCIA', 'ARAGON', 'CANARIAS' ,'CASTILLA Y LEON', 'CASTILLA-LA MANCHA',
                              'CATALUNA', 'COMUNIDAD DE MADRID', 'COMUNIDAD FORAL DE NAVARRA',
                              'COMUNITAT VALENCIANA', 'EXTREMADURA' ,'GALICIA', 'ILLES BALEARS', 'LA RIOJA',
                              'PAIS VASCO', 'REGION DE MURCIA']
    ccaa_hidraulica = ['ANDALUCIA', 'ARAGON' ,'CANARIAS' ,'CANTABRIA', 'CASTILLA Y LEON',
                       'CASTILLA-LA MANCHA', 'CATALUNA', 'COMUNIDAD FORAL DE NAVARRA',
                       'COMUNITAT VALENCIANA' ,'GALICIA','LA RIOJA', 'PAIS VASCO',
                       'PRINCIPADO DE ASTURIAS' ,'REGION DE MURCIA', 'EXTREMADURA']
    ccaa_eolica = ['ANDALUCIA' ,'ARAGON' ,'CANTABRIA' ,'CASTILLA Y LEON' ,'CASTILLA-LA MANCHA',
                   'CATALUNA', 'COMUNIDAD DE MADRID', 'COMUNIDAD FORAL DE NAVARRA',
                   'COMUNITAT VALENCIANA' ,'EXTREMADURA' ,'GALICIA', 'LA RIOJA', 'PAIS VASCO',
                   'PRINCIPADO DE ASTURIAS', 'REGION DE MURCIA']

    #Based on the information provided by the Pndas Profile and the other notebooks, we can drop alredy some variables that we know are not relevant for each technology:
    
    pred_solartermica = predictions_dataframe[predictions_dataframe['comunidad_autonoma'].isin(ccaa_solartermica)]
    pred_solartermica = pred_solartermica[['fecha', 'comunidad_autonoma','tmed','tmax','velmedia','sol', 'inst_Solar_térmica']] \
        .rename(columns = {'Solar térmica':'generation'})
    pred_solarfotovoltaica = predictions_dataframe[predictions_dataframe['comunidad_autonoma'].isin(ccaa_solarfotovoltaica)]
    pred_solarfotovoltaica = pred_solarfotovoltaica[['fecha', 'comunidad_autonoma','tmed','tmax','velmedia','sol', 'inst_Solar fotovoltaica']] \
        .rename(columns = {'Solar fotovoltaica':'generation'})
    pred_hidraulica = predictions_dataframe[predictions_dataframe['comunidad_autonoma'].isin(ccaa_hidraulica)]
    pred_hidraulica = pred_hidraulica[['fecha','comunidad_autonoma','tmed','AGUA_ACTUAL','AGUA_TOTAL', 'inst_Hidráulica']] \
        .rename(columns = {'Hidráulica':'generation'})
    pred_eolica = predictions_dataframe[predictions_dataframe['comunidad_autonoma'].isin(ccaa_eolica)]
    pred_eolica = pred_eolica[['fecha', 'comunidad_autonoma','tmed', 'velmedia', 'racha', 'inst_Eólica']] \
        .rename(columns = {'Eólica':'generation'})
    pred_demand = predictions_dataframe[['fecha', 'comunidad_autonoma', 'tmed', 'sol', 'Weekday',]]

    #We take the opportunity to do already here the OneHotEncoder:

    pred_solartermica = onehotencoder_ccaa(pred_solartermica)
    pred_solarfotovoltaica = onehotencoder_ccaa(pred_solarfotovoltaica)
    pred_hidraulica = onehotencoder_ccaa(pred_hidraulica)
    pred_eolica = onehotencoder_ccaa(pred_eolica)
    pred_demand = onehotencoder_ccaa(pred_demand)
    #The only CCAA we don't analize:
    pred_demand.drop(columns = 'comunidad_autonoma_CEUTA', inplace =True)
    return pred_solartermica, pred_solarfotovoltaica, pred_hidraulica, pred_eolica, pred_demand

def cod_provincias(cod_provincias_df):

    """
    In order to transform codprovincias dataframe so it can be crossmatched with other files, we haver several things to adapt in columns format:
    """

    cod_provincias_df.rename(columns = {'Provincia': 'provincia'},inplace= True)
    cod_provincias_df['provincia'] = cod_provincias_df['provincia'].apply(unidecode).str.upper()
    cod_provincias_df['Comunidad Autónoma'] = cod_provincias_df['Comunidad Autónoma'].apply(unidecode).str.upper()
    cod_provincias_df = cod_provincias_df.rename(columns = {'Comunidad Autónoma':'comunidad_autonoma'})
    return cod_provincias_df


def power_installed_last_month(powerinstalled_file):
    lastmonth = powerinstalled_file['fecha'].max()
    power_file = powerinstalled_file[powerinstalled_file['fecha'] == lastmonth]
    power_file = power_file.rename(columns= {'Eólica': 'inst_Eólica',
                                                       'Hidráulica': 'inst_Hidráulica',
                                                       'Solar fotovoltaica': 'inst_Solar fotovoltaica',
                                                       'Solar térmica': 'inst_Solar_térmica'
                                                       })

    return power_file

def pivot_table_generation_by_ccaa(generation_total_by_ccaa_file):
    """
    Transposing the category column 'type' into several columns with 'value_ccaa' as values.
    """

    #We create the pivot table: only with the columnd we are interested in:
    generation_total_by_ccaa_file =  generation_total_by_ccaa_file.pivot_table(index = ['fecha', 'comunidad_autonoma'],
                                                            columns = 'Type',
                                                            values = 'value_ccaa',
                                                            fill_value = 0).reset_index()
    

    #In order to remove 'Type' index column that has been inserted automatically with the pivot table:

    generation_total_by_ccaa_file = generation_total_by_ccaa_file.reset_index(drop = True)
    generation_total_by_ccaa_file.columns.name = None
    
    return generation_total_by_ccaa_file

def REE_ccaa_rename(dataframe):
    dataframe['comunidad_autonoma'] = dataframe['comunidad_autonoma'].replace({'CASTILLA- LA MANCHA': 'CASTILLA-LA MANCHA',
                                                                                          'COMUNIDAD DE NAVARRA': 'COMUNIDAD FORAL DE NAVARRA',
                                                                                          'COMUNIDAD MELILLA':'MELILLA',
                                                                                          'COMUNIDAD VALENCIANA': 'COMUNITAT VALENCIANA',
                                                                                          'ISLAS BALEARES': 'ILLES BALEARS',
                                                                                          'ISLAS CANARIAS': 'CANARIAS',
                                                                                          })
    return dataframe

def powerinstalled_CCAA_process(powerfile):

        """
        Dropping columns not needed, decoding text and transforming dates into a friendly format.
        Selecting the technologies we are using.
        """
        
        powerfile = powerfile.drop(columns = [col for col in powerfile.columns if col =='Unnamed: 0'])

        #As we have made for the generation_total file, we need the period to cross data instead of just 'month':

        powerfile['month'] = powerfile['month'].str[:10]
        powerfile['month'] = pd.to_datetime(powerfile['month'])

        #In order to cross with other files by community name:

        powerfile['comunidad_autonoma'] = powerfile['comunidad_autonoma'].apply(unidecode).str.upper()

        #Filter only the techs we want and drop 'todas' from comunidad autonoma

        technologies_to_use = ['Solar térmica','Hidráulica','Solar fotovoltaica','Eólica']
        powerfile = powerfile[powerfile['type'].isin(technologies_to_use)]
        powerfile = powerfile[powerfile['comunidad_autonoma'] != 'TODAS']

        #'type' column has to be renamed also:

        powerfile = powerfile.rename(columns={'type': 'Type',
                                              'value': 'value_ccaa',
                                              'month': 'fecha'})

        return powerfile

def all_predictions (predictions_file, embalses_file, power_installed_file): #To finally have the file ready to work with it:

    """ 
    Function to cross all dataframes AEMET, MITECO & REE regarding predictions and create the final dataframe
    Creating the column 'Weekday' to incorporate it into 'demand' file.
    """
    
    predictions_total = pd.merge(predictions_file, embalses_file, how = 'left', on = 'comunidad_autonoma')

    #Now we cross it with codprovincias file:

    predictions_total = pd.merge(predictions_total, power_installed_file, how = 'left', on= 'comunidad_autonoma')

    #Let's transform 'fecha' into datetime:

    predictions_total['fecha'] = pd.to_datetime(predictions_total['fecha'])

    #Replacing NAs for water info:
    predictions_total['AGUA_TOTAL'] = predictions_total['AGUA_TOTAL'].fillna(0)
    predictions_total['AGUA_ACTUAL'] = predictions_total['AGUA_ACTUAL'].fillna(0)

    #We are including also the column 'weekday' which will help us when estimating demand:

    predictions_total['Weekday'] = predictions_total['fecha'].dt.day_name().apply(lambda x:1 if x in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'] else 0)

    return predictions_total

def fill_na_predictions (predictions_file):

    predictions_file.fillna({
        'AGUA_TOTAL': 0,
        'AGUA_ACTUAL':0,
        'inst_Eólica':0,
        'inst_Hidráulica':0,
        'inst_Solar fotovoltaica':0,
        'inst_Solar_térmica':0,
    }, inplace = True )
    predictions_file['racha'].fillna(predictions_file['racha'].mean(), inplace=True)
    predictions_file['sol'].fillna(predictions_file['sol'].mean(), inplace=True)
    
    return predictions_file

def onehotencoder_ccaa (dataframe):

    #It gets a dataframe incldugin severla variables and a column called 'comunidad_autonoma' and returns the same dfwith that column encoded for ML trainging.

    dataframe_enc = dataframe.copy()
    enc = OneHotEncoder() 
    encoded_feature = enc.fit_transform(dataframe_enc[['comunidad_autonoma']]).toarray() #it only works with arrays
    df_feature = pd.DataFrame(encoded_feature, columns = enc.get_feature_names_out(['comunidad_autonoma']))
    dataframe_enc = dataframe_enc.drop('comunidad_autonoma', axis = 1)
    dataframe_enc = dataframe_enc.reset_index(drop=True)
    df_encoded = pd.concat([dataframe_enc, df_feature], axis = 1)
    return df_encoded

def sort_municipios(municipios_df):
    
    """Select some samples (3) for every province based on nº inhabitants"""

    #Firstly, we want to extract 'province id' from the id for each 'municipio':
    
    municipios_df['prov_id'] = municipios_df['id'].str[2:4]

    #Transform nª inhabitants for sorting
    municipios_df['num_hab'] = pd.to_numeric(municipios_df['num_hab'])

    # Sort the entire dataframe by 'prov_id' and 'num_hab'
    municipios_ranked = municipios_df.sort_values(by=['prov_id', 'num_hab'], ascending=[True, False])

    # Group by 'prov_id' and pick top 5 for each group
    top_municipios = municipios_ranked.groupby('prov_id').head(3)

    top_municipios = top_municipios.sort_values(by='num_hab', ascending=False)

    # Reset index for the final dataframe
    top_municipios = top_municipios.reset_index(drop=True)

    top1 = pd.DataFrame(columns=top_municipios.columns)
    top2 = pd.DataFrame(columns=top_municipios.columns)
    top3 = pd.DataFrame(columns=top_municipios.columns)

    for index, row in top_municipios.iterrows():
        if row['prov_id'] not in top1['prov_id'].unique():
            top1 = pd.concat([top1, row.to_frame().T])
        else:
            if row['prov_id'] not in top2['prov_id'].unique():
                top2 = pd.concat([top2, row.to_frame().T])
            else:
                top3 = pd.concat([top3, row.to_frame().T])
    
    top1 = pd.DataFrame(top1)
    top2 = pd.DataFrame(top2)
    top3 = pd.DataFrame(top3)
    
    top_municipios_df = pd.concat([top1, top2, top3])

    # Reset index for the final dataframe
    top_municipios_df = top_municipios_df.reset_index(drop=True)

    return top_municipios_df

def embalses_elect (embalses_file, presas_file):

    """
    It processes two dataframes, one with the historical data of the water reservoirs in Spain and a second one with the masterdata of those dams. The result is a cleaner dataframe with the columns we need:
    - Select only the information of the dams that produce hidroelectric.
    - Modify variables into numeric format.
    - Decode text to have it the same way as other files in order to cross it.
    -Cross both files to have the information of each embalse and its status.
    - Group the dataframe by fecha and provinca to sum up the values so it can be crossed afterwards.
    - Adjust 'provincia' & 'fecha' fields.

    """

    #Firstly, we read the 'embalses' data and select only the year we want and to process 'Embalse Nombre' in order to match it with 'presas' file.
    #We only want the dams with the electricty flag marked
    
    embalses_elect = embalses_file.loc[embalses_file['ELECTRICO_FLAG'] == 1]
    embalses_elect['year'] = pd.DatetimeIndex(embalses_elect['FECHA']).year

    #In order to transform water KPIs into numeric
    str(embalses_elect['AGUA_TOTAL'])
    embalses_elect['AGUA_TOTAL'] = embalses_elect['AGUA_TOTAL'].str.replace(',' , '.')
    embalses_elect['AGUA_TOTAL'] = embalses_elect['AGUA_TOTAL'].apply(pd.to_numeric)
    str(embalses_elect['AGUA_ACTUAL'])
    embalses_elect['AGUA_ACTUAL'] = embalses_elect['AGUA_ACTUAL'].str.replace(',' , '.')
    embalses_elect['AGUA_ACTUAL'] = embalses_elect['AGUA_ACTUAL'].apply(pd.to_numeric)

    #In order to have the information of the year that we want and to get the name of the dam in the way to match it with 'presas' file

    embalses_elect = embalses_elect[embalses_elect['year'] != 2014]
    embalses_elect['EMBALSE_NOMBRE'] = embalses_elect['EMBALSE_NOMBRE'].apply(str.upper).apply(unidecode)

    #Now, we have to preprocess 'presas' data

    presas_file.rename({'Presa':'EMBALSE_NOMBRE'}, axis = 1, inplace = True)
    presas_file['EMBALSE_NOMBRE'] = presas_file['EMBALSE_NOMBRE'].apply(unidecode)

    #At last, the joint. It will be some dams not matched but are not relevant for our study:

    embalses_capacity = embalses_elect.merge(presas_file, on = 'EMBALSE_NOMBRE', how= 'inner')

    #In the end, we want the information aggregated by 'Provincia'. Therefore:

    embalses_capacity = embalses_capacity.groupby(by =['FECHA','Provincia'], as_index =False)[['AGUA_TOTAL', 'AGUA_ACTUAL']].sum().sort_values(by = 'FECHA', ignore_index = True)

    #We are also changing here the name 'Coruña, A' in this function as it is necessary to cross it with other files:

    embalses_capacity['Provincia'] = embalses_capacity['Provincia'].replace('Coruña, A' ,'A Coruña')

    #In order to have'Provincias' and 'FECHA' in the same form as the other files:

    embalses_capacity = embalses_capacity.rename(columns = {'Provincia': 'provincia', 'FECHA': 'fecha'})
    embalses_capacity['provincia'] = embalses_capacity['provincia'].apply(unidecode).str.upper()
    
    return embalses_capacity







#MODELS

def lr_model (X,y):

    #Estimator would be LR from sklearn

    reg = LinearRegression()

    # Get the number of the features we are including to reshape accordingly afterwards
    shape = X.shape[1]

    #Reshape in order to have data adjusted

    X = X.reshape(-1,shape)
    y = y.reshape(-1,1)

    print(X.shape)
    print(y.shape)
    
    #Fit in order to train the data
    reg.fit(X,y)

    #We split now the data into train and test
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.15)

    #Metrics of the model we want to know

    predictions = reg.predict(X).round(2)
    MAE = np.round(mean_absolute_error(y_test, reg.predict(X_test)),2)
    r2 = np.round(r2_score(y_test, reg.predict(X_test)),3)
    MSE = np.round(mean_squared_error(y_test,reg.predict(X_test), squared = True),2)
    RMSE = np.round(mean_squared_error(y_test,reg.predict(X_test), squared = False),2)
    cv = cross_val_score(reg, X, y, cv = 5)
    cv_mean = np.round(cv.mean(), 3)
    cv_std = np.round(cv.std(), 3)


    #Outputs
    
    print(f'Model intercept is {reg.intercept_.round(2)}')
    print(f'Model coefficient is {reg.coef_.round(2)}')
    print(f'Model MAE is {MAE}')
    print(f'Model MSE is {MSE}')
    print(f'Model r2 is {r2}')
    print(f'Model RMSE is {RMSE}')
    print(f'CV mean is {cv_mean} and std is {cv_std}')

    return predictions

def knn_model (X,y):

    from sklearn.neighbors import KNeighborsRegressor

    #Estimator would be KNN from sklearn

    reg = KNeighborsRegressor()

    # Get the number of the features we are including to reshape accordingly afterwards
    shape = X.shape[1]

    #Reshape in order to have data adjusted

    X = X.reshape(-1,shape)

    print(X.shape)
    print(y.shape)
    
    #Fit in order to train the data
    reg.fit(X,y)

    #We split now the data into train and test
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.15)

    #Metrics of the model we want to know

    predictions = reg.predict(X).round(2)
    MAE = np.round(mean_absolute_error(y_test, reg.predict(X_test)),2)
    r2 = np.round(r2_score(y_test, reg.predict(X_test)),3)
    MSE = np.round(mean_squared_error(y_test,reg.predict(X_test), squared = True),2)
    RMSE = np.round(mean_squared_error(y_test,reg.predict(X_test), squared = False),2)
    cv = cross_val_score(reg, X, y, cv = 5)
    cv_mean = np.round(cv.mean(), 3)
    cv_std = np.round(cv.std(), 3)


    #Outputs
    
 
    print(f'Model MAE is {MAE}')
    print(f'Model MSE is {MSE}')
    print(f'Model r2 is {r2}')
    print(f'Model RMSE is {RMSE}')
    print(f'CV mean is {cv_mean} and std is {cv_std}')

    return predictions

def rforest_model (X,y):

    #Estimator would be RandomForest from sklearn

    reg = RandomForestRegressor()

    # Get the number of the features we are including to reshape accordingly afterwards
    shape = X.shape[1]

    #Reshape in order to have data adjusted

    X = X.reshape(-1,shape)

    print(X.shape)
    print(y.shape)
    
    #Fit in order to train the data
    reg.fit(X,y)

    #We split now the data into train and test
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.15)

    #Metrics of the model we want to know

    predictions = reg.predict(X).round(2)
    MAE = np.round(mean_absolute_error(y_test, reg.predict(X_test)),2)
    r2 = np.round(r2_score(y_test, reg.predict(X_test)),3)
    MSE = np.round(mean_squared_error(y_test,reg.predict(X_test), squared = True),2)
    RMSE = np.round(mean_squared_error(y_test,reg.predict(X_test), squared = False),2)
    cv = cross_val_score(reg, X, y, cv = 5)
    cv_mean = np.round(cv.mean(), 3)
    cv_std = np.round(cv.std(), 3)


    #Outputs
    
 
    print(f'Model MAE is {MAE}')
    print(f'Model MSE is {MSE}')
    print(f'Model r2 is {r2}')
    print(f'Model RMSE is {RMSE}')
    print(f'CV mean is {cv_mean} and std is {cv_std}')

    return predictions

def apply_model (df_prediction, model):
    dates= df_prediction['fecha']
    df_prediction = df_prediction.drop(columns = 'fecha')
    df_prediction['prediction'] = model.predict(df_prediction)
    df_prediction['fecha'] = dates

    return df_prediction

def standarization_standard_scaler (dataframe):

    dataframe_st = dataframe.copy()
    scaler = StandardScaler()
    #In order to standarize values of the dataframes and get better results in the model by removing the mean and scaling to unit variance.
    #We don't want to apply standarization to target value or categorical features as it may have an impact on the result:

    cols_to_standarize = [col for col in dataframe_st.columns if dataframe_st[col].dtype in ['int64', 'float64'] \
                          and col != ['generation','demand_ccaa', 'fecha'] \
                            and not col.startswith('comunidad')]

    for col in cols_to_standarize:
        dataframe_st[col] = scaler.fit_transform(dataframe_st[[col]])
    
    return dataframe_st

def standarization_minmax_scaler (dataframe):

    dataframe_st = dataframe.copy()
    scaler = MinMaxScaler()
    #It allows to scale data to range (0,1)
    #We don't want to apply standarization to target value or categorical features as it may have an impact on the result:

    cols_to_standarize = [col for col in dataframe_st.columns if dataframe_st[col].dtype in ['int64', 'float64'] \
                        and col != ['generation','demand_ccaa'] \
                        and not col.startswith('comunidad')]

    for col in cols_to_standarize:
        dataframe_st[col] = scaler.fit_transform(dataframe_st[[col]])
    
    return dataframe_st



#READ DATA

def drive_read_file(url):
    for retry in range(3):
        try:
            # Generate the Google Drive download URL
            download_url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
            
            # Download the file using gdown
            file_path = gdown.download(download_url, quiet=False)
            
            if file_path is not None:
                # Read the file directly into a pandas DataFrame
                df = pd.read_csv(file_path)
                
                # Delete the downloaded file
                os.remove(file_path)
                
                return df
            else:
                print("Download failed. Trying again...")
                time.sleep(5.0)  # Wait for another retry
        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry
    print("Unable to download. Please try again later.")
    return None

def read_gpd_file(url):
    file_id = url.split('/')[-2]
    url_total = 'https://drive.google.com/uc?export=download&id=' + file_id
    
    for retry in range(3):
        try:
            r = requests.get(url_total)
            r.raise_for_status() 
            
            if r.status_code == 200:
                
                temp_dir = tempfile.mkdtemp()
                with open(f"{temp_dir}/file.zip", "wb") as f:
                    f.write(r.content)
                with zipfile.ZipFile(f"{temp_dir}/file.zip", 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                shapefile = [f for f in os.listdir(temp_dir) if f.endswith('.shp')][0]
                map_file = gpd.read_file(f"{temp_dir}/{shapefile}")
                shutil.rmtree(temp_dir)
                return map_file
            else:
                print("Server error. Trying again...")
                time.sleep(5.0)  # Wait for another retry
        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry
    
    
    print("Unable to download, server not working. Please try again later.")
    return None

def drive_read_file_othersep(url, sep=';'):
    for retry in range(3):
        try:
            # Generate the Google Drive download URL
            file_url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
            
            # Download the file and read it into a DataFrame
            df = pd.read_csv(file_url, sep=sep)
            
            return df
        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry
    
    print("Unable to download and read the file. Please try again later.")
    return None

def drive_read_xlsx_file(url):
    for retry in range(3):
        try:
            # Generate the Google Drive download URL
            download_url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
            
            # Download the file using gdown
            file_path = gdown.download(download_url, quiet=False)
            
            if file_path is not None:
                # Read the file directly into a pandas DataFrame
                df = pd.read_excel(file_path)
                
                # Delete the downloaded file
                os.remove(file_path)
                
                return df
            else:
                print("Download failed. Trying again...")
                time.sleep(5.0)  # Wait for another retry
        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry
    
    print("Unable to download and read the file. Please try again later.")
    return None

def drive_read_latin_encoding_file(url): #In order to download the files with the urls provided at the beginning of the notebook and that are uin xslsx format:

    for retry in range(3):
        try:

            # Generate the Google Drive download URL
            download_url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
            
            # Download the file using gdown
            file_path = gdown.download(download_url, quiet=False)

            if file_path is not None:
            
                # Read the file directly into a pandas DataFrame
                df = pd.read_csv(file_path, encoding = 'latin-1', sep = ';')
                
                # Delete the downloaded file
                os.remove(file_path)
                return df
            else:
                print("Download failed. Trying again...")
                time.sleep(5.0)  # Wait for another retry

        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry

    print("Unable to download and read the file. Please try again later.")
    return None


def drive_read_joblibmodel(url): #In order to download the trained models

    # Generate the Google Drive download URL
    download_url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]

    for retry in range(3):
        try:

            # Download the file using gdown
            file_path = gdown.download(download_url, quiet=False)

            if file_path is not None:
            
                # Read the file directly into a pandas DataFrame
                model = load(file_path)
                
                # Delete the downloaded file
                os.remove(file_path)

                return model
            
            else:
                print("Download failed. Trying again...")
                time.sleep(5.0)  # Wait for another retry

        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry

    print("Unable to download and read the file. Please try again later.")
    return None
    


def drive_read_image(url):

    # Generate the Google Drive download URL
    download_url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]

    for retry in range(3):

        try:

            # Download the file using gdown
            file_path = gdown.download(download_url, quiet=False)

            if file_path is not None:
            
                # Read the file directly into a pandas DataFrame
                image = Image.open(file_path)

                return image
            else:
                    print("Download failed. Trying again...")
                    time.sleep(5.0)  # Wait for another retry

        except Exception as e:
            print(f"Error {e}. Trying again...")
            time.sleep(5.0)  # Wait for another retry 
    
    print("Unable to download and read the file. Please try again later.")
    return None







#PLOTS

def plot_boxplot(dataframe, column_names):
    # Select the specified columns from the DataFrame
    subset_df = dataframe[column_names]

    # Create a boxplot
    plt.figure(figsize=(10, 6))  
    sns.boxplot(data = subset_df)

    plt.title('Energy Production Boxplot')  
    plt.xlabel('Energy Sources')  
    plt.ylabel('Energy Production') 

    plt.show()

def plot_map_generation(map_file, dataframe, year, plotlimits):

    dataframe = dataframe[dataframe['fecha'].dt.year == year]
    dataframe = dataframe.groupby('comunidad_autonoma')['generation'].sum()
    spain = pd.merge(map_file, dataframe, on='comunidad_autonoma', how='left').fillna(0)

    #Setup the color

    color_start = "#F3F3F3"
    color_end = "#21D67C"

    cmap = LinearSegmentedColormap.from_list('EFAT_color', [color_start, color_end])

    #Transform generation into GWh:

    spain['generation'] = spain['generation']/1000

    # Plot:
    fig, ax = plt.subplots(figsize=(14, 10))

    # Plot the boundaries between comunidades autónomas
    spain['geometry'].boundary.plot(ax=ax, linewidth=0.5, color='black')

    #Nseeting the value limits for generation:

    norm = Normalize(vmin=plotlimits[0], vmax=plotlimits[1])

    # Use ax parameter for both plots
    spain.plot(column='generation', legend=True, ax=ax, cmap = cmap, norm = norm)
    
    # Adjust the map limits
    ax.set_xlim(-16, 5) 
    ax.set_ylim(33, 45)

    plt.title(f'Generation of by CCAA in GWh, {year}')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    plt.show()

    return fig

def plot_map_demand(map_file, dataframe, year, plotlimits):

    dataframe = dataframe[dataframe['fecha'].dt.year == year]
    dataframe = dataframe.groupby('comunidad_autonoma')['demand_ccaa'].sum()
    spain = pd.merge(map_file, dataframe, on='comunidad_autonoma', how='left').fillna(0)

    #Setup the color

    color_start = "#F3F3F3"
    color_end = "#21D67C"

    cmap = LinearSegmentedColormap.from_list('EFAT_color', [color_start, color_end])

    #Transform generation into GWh:

    spain['demand_ccaa'] = spain['demand_ccaa']/1000

    # Plot:
    fig, ax = plt.subplots(figsize=(14, 10))

    # Plot the boundaries between comunidades autónomas
    spain['geometry'].boundary.plot(ax=ax, linewidth=0.5, color='black')

    # Use ax parameter for both plots
    spain.plot(column='demand_ccaa', legend=True, ax=ax, cmap = cmap)
    
    # Adjust the map limits
    ax.set_xlim(-16, 5) 
    ax.set_ylim(33, 45)

    plt.title(f'Demand of by CCAA in GWh, {year}')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.legend()
    plt.show()

    return fig

def color_theme():
    color_start = "#F3F3F3"
    color_end = "#21D67C"

    cmap = LinearSegmentedColormap.from_list('EFAT_color', [color_start, color_end])

    return cmap

def plot_bars_predictions(df_predictions):

    fig, ax = plt.subplots()
    ax.bar(x=df_predictions['fecha'], height=df_predictions['prediction'], color='#21D67C')

    plt.ylabel('MW/h Prediction')
    plt.xlabel('Date')
    ax.tick_params(axis = 'x', rotation = 45)
    ax.set_ylim(0,850000)
    ax.spines['bottom'].set_color('#808080')
    ax.spines['top'].set_color('#808080')
    ax.spines['left'].set_color('#808080')
    ax.spines['right'].set_color('#808080')

    ax.tick_params(axis='x', colors='#808080')
    ax.tick_params(axis='y', colors='#808080')
    fig.patch.set_facecolor('#F3F3F3')

    return fig

def plot_completion(values):

    size = 0.3
    fig,ax = plt.subplots()
    ax.pie(values, radius = 1, colors = [ 'grey', '#21D67C'], wedgeprops = dict(width = size, edgecolor = '#F3F3F3'))
    plt.text(0, 0, f'{values[1]}%', ha='center', va='center', fontsize=20)
    plt.legend([ 'Non renewable generation','Renewable Generation'], loc = 'lower center')
    fig.patch.set_facecolor('#F3F3F3')


    return fig