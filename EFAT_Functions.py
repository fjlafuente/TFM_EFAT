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
import statistics
import time

























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

def data_REE_potencia_instalada(yearini, yearend): # -> REE API Only allows to extract data in a yearly range as much, in monthly basis


    #First we get the response from REE (It only allow us to see one year each time). We will create an empty list that will be appended

    years = range(yearini,yearend + 1)
    all_data = []

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
    
    return data_consolidated

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

    #In order to download and to update pur 'Embalses' info file which will contain the basic data for hidro estimations

    #The file is stored in 'Ministerio para la Transición Ecológica y reto demográfico' webpage, and it is stored in zip format

    embalses = requests.get('https://www.miteco.gob.es/es/agua/temas/evaluacion-de-los-recursos-hidricos/bd-embalses_tcm30-538779.zip')
    with zipfile.ZipFile(io.BytesIO(embalses.content), 'r') as zip_embalses:
        zip_embalses.extractall('./Data/Hidro/')

    #When extracting it we get a mdb file. To transform it to a csv file:

    # MDB file path
    mdb_file = './Data/Hidro/BD-Embalses.mdb'

    # table name. It has to be adjusted within the years
    table_name = "T_Datos Embalses 1988-2023"

    # output CSV file path
    output_csv_file = './Data/Hidro/Embalses.csv'

    command = f"mdb-export {mdb_file} \"{table_name}\""
    output = subprocess.run(shlex.split(command), capture_output=True, text=True).stdout

    # Write the output to the CSV file
    with open(output_csv_file, 'w') as f:
        f.write(output)

    #Convert the csv file into a dataframe

    embalses_data = pd.read_csv('./Data/Hidro/Embalses.csv')

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
    
    response = requests.request("GET", url,  params = query)

    aemet_municipios = response.json()

    #The response is a list of dicts, in which each dict is one municipio:

    for item in aemet_municipios:

        municipio = pd.DataFrame(item, index= [0])
        aemet_municipios_total = pd.concat([aemet_municipios_total, municipio])
    aemet_municipios_total.set_index('id', inplace = True)

    aemet_municipios_total.to_csv('Data/Weather/Municipios_md.csv')

def aemet_municipios_predictions(): # -> in order to get predictions for next seven days from AEMET API

    #We are going to extract the predictions of all municipios in Spain

    municipios = pd.read_csv('Data/Weather/Municipios_md.csv')

    #As we are not going to be able to access to all data due to time cand resources consumption, we ordered by num.inhabitants to get to as many provinces as possible

    municipios = municipios.sort_values('num_hab', ascending = False)

    municipios_id = list(municipios['id'])

    #We create a dataframe that will be appended

    df_prediction = pd.DataFrame(columns= ['id_municipio', 'nombre', 'provincia', 'fecha', 'tmax', 'tmin', 'estado_cielo', 'viento', 'racha'])

    #Now we create a loop in order to access to all the required information for each municipio:
    e = 0

    nprovincias = []



    for municipio in municipios_id:
        e+=1
        
        if e > 130:
            break
        else:
            #In order to avoid blocking AEMET servers:

            time.sleep(0.3)

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

                predictions = response.json()['datos']

                nprovincias.append(municipio[:2])
                

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
                        datadict = {'id_municipio': item_id, 'nombre': nombre, 'provincia':provincia, 'fecha': fecha, 'tmax': tmax, 'tmin':tmin, 'estado_cielo' : estado_cielo, 'viento': viento, 'racha' :racha}
                        total_prediction = pd.DataFrame(datadict, index = [0])
                        df_prediction = pd.concat([df_prediction, total_prediction])
                
                
                    

            else:
                predictions = response.json()['datos']

                nprovincias.append(municipio[:2])
                

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
                        datadict = {'id_municipio': item_id, 'nombre': nombre, 'provincia':provincia, 'fecha': fecha, 'tmax': tmax, 'tmin':tmin, 'estado_cielo' : estado_cielo, 'viento': viento, 'racha' :racha}
                        total_prediction = pd.DataFrame(datadict, index = [0])
                        df_prediction = pd.concat([df_prediction, total_prediction])
       
                               
        

    print(f'{len(set(nprovincias))}, {e}')
    return df_prediction

def weather_csv_file(year): # -> So we can save all years data in our project's directory
    weather = aemet_data_api(year)
    weather.to_csv(f"~/data/TFM_EFAT/TFM_EFAT/Data/Weather/Weather{year}.csv", index = False)

## PROCESSING

def weather_processing(weather_file):

    weather_file_processed = weather_file

    #Date to datetime:

    weather_file_processed['fecha'] = weather_file_processed['fecha'].apply(to_datetime)

    #In order to replace 'Ip' values in 'prec':

    weather_file_processed.loc[weather_file_processed['prec'] == 'Ip', 'prec'] = 0
    
   #Let's transform columns into numeric: 

    columns_to_transform = ['prec', 'velmedia', 'racha', 'tmed', 'tmin', 'tmax', 'sol']
    
    for column in columns_to_transform:
        weather_file_processed[column] = weather_file_processed[column].str.replace(',', '.')
        weather_file_processed[column] = weather_file_processed[column].apply(pd.to_numeric)

    return weather_file_processed

def constrain_weather (weather_file): 

    #In order to simplify the data, eliminating the geo info by making the mean per day
    
    weather_pivot_table = weather_file[['fecha', 'provincia', 'tmed', 'prec', 'tmin', 'tmax', 'velmedia', 'racha', 'sol']]
    weather_pivot_table = weather_file.groupby(['fecha', 'provincia'], as_index=False)[['tmed', 'prec', 'tmin', 'tmax', 'velmedia', 'racha', 'sol']].mean()
    weather_pivot_table = weather_pivot_table.groupby('fecha', as_index = False)[['tmed', 'prec', 'tmin', 'tmax', 'velmedia', 'racha', 'sol']].mean()

    return weather_pivot_table

def embalses_elect_year (year):

    #Firstly, we read the 'embalses' data and select only the year we want and to process 'Embalse Nombre' in order to match it with 'presas' file.
    #We only want the dams with the electricty flag marked
    
    embalses_elect = pd.read_excel('./Data/Hidro/T_Embalses_2014_2023.xlsx')
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

    presas = pd.read_csv('./Data/Hidro/Presas.csv', encoding = 'latin-1', sep = ';')
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