# TFM_EFAT project

## Description of the project

Electricity Forecasting and Analysis Tool (EFAT) project is a tool created within the KSchool Data Science master with the aim of providing users with a tool to predict the estimated energetic generation in Spain based on weather predictions. 

As rewenable energy is increasing its proudction year after year and Spain and other developed countries are looking for reducing completely the use of non renewable resources, and taking into consideration how the pricing works in electricty market () EFAT can be a helpful tool in order to predict how the market can evolve in the near future.

In this analysis we will use information related to:

    - Energetic Generation: extracted from Red Eléctrica de España (REE) website in several files (on a yearly basis)
    - Weather historical data: from AEMET API, it contains information related to the meteo stations of the whole country.
    - Provincias: As we want to take into consideration geo data but it is complicated to have all the detail, we are considering 'Provincias' as geo reference for the data we are analyzing.
    - Power Installed: It's a critical variable and it is provided also by REE in a monthly basis.
    - Weather_Predictions: It will be the 'food' for the model to work. The test data that we want to predict.
    - 'Embalses / Dams' Info: Critical for hidroelectric production.

The data related to egenery production and demand is extracted from 'Red Electrica de España' website: https://www.ree.es/es

Weather data is also public and it can be downloaded from AEMET OpenData website: https://opendata.aemet.es/centrodedescargas/inicio

A more detailed explanation of the project's goal' and the main steps followed can be found on the folder 'doc' of this repository, in the Memory doc.

## Respository structure:

In the repository you will find:

    - requirements.txt : Includes all the specifications needed for the projject, to be installed ith pip.
    - doc: Folder in which the project's Memory is saved and also it can be found another file from REE useful to understand better the project.
    - APP: Frontend (Streamlit app) source code folder.
    - EFAT_Functions.py: Code file with all the functions that are used on the project, useful for frontend app or to easily implement new code. 
    - Exploration_notebooks: Folder with the noteeboks used to take a first look at all the data, considering only 2014 for this exercise.
    - EFAT_Model_Conclusions: Main notebook of the project, in which the processses of downlading the data, processing and visualizing it are explained and the models ara calculate.
    - Processed_Dataframes: Information of each technology already processed and ready to be used on the frontend.
    - EFAT_predictions: notebook in which the weather online is downloaded and processed and the models calculated are applied to get the predictions we are looking for.

## Google Drieve shared folder:

Almost all the data is stored online on a public Google Drive folder, which males easier and lighter to work withdata without having to download it to local.
Data can be found here:

[Google Drive EFAT DAta Folder](https://drive.google.com/drive/folders/1Y36_4Z-JY7Ig6lpNZeMfToooRL12pdvo?usp=drive_link)

But the specific URLs that are used are mentioned in the notebooks.

## How to start

1. First of all, clone this repository into your local, doing it on the terminal or downloading it as a zipfile.

Remember than data is stored on Google Drive's shared folder, notebooks and streamlit APP directly download the information from there.

The easiest and most simple way to start working with EFAT project is creating an virtual environment and installing all the specifications included in requeriments.txt

2. Creating an envioronment is easier with Conda trhough its web:

[Anaconda Download](https://www.anaconda.com/download)

3. Then it is easy to create a new environment by launching AnacondaPrompt (or other terminal):

```conda create --name EFAT_env python=3.8.11 ```

4. Then, once the environment is created, it's time to install all the packages needed.
Firstly, activate Conda environment:

```conda activate EFAT_env```

5. Then go to the folder in which the repository has been cloned and install requirements.txt

`pip install -r requirements.txt`

Now you have everything you need to start learning with EFAT project!
A good idea could be to start taking a look at the Exploration notebooks or directly start with 'EFAT_Model_Conclusions' notebook. After that, you could find how the model is implemented with 'EFAT_Predictions'.
In case you are just curious about the final ouput of the project you can go directly to the online frontend built on streamlit:



Hope you enjoy!



