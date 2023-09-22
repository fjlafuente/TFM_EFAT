# TFM_EFAT

Electricity Forecasting and Analysis Tool is a tool created within the KSchool Data Science master with the aim of providing users with a tool to predict the estimated energetic generation in Spain based on weather predictions. 

As rewenable energy is increasing its proudction year after year and Spain and other developed countries are looking for reducing completely the use of not renewable resources, and taking into consideration how the pricing works in electricty market () EFAT can be a helpful tool in order to predict how the market can evolve in the near future.


The data related to egenery production and demand is extracted from 'Red Electrica de España' website: https://www.ree.es/es

Weather data is also public and it can be downloaded from AEMET OpenData website: https://opendata.aemet.es/centrodedescargas/inicio

The main notebook of the project is the one called 'EFAT_Model_Conclussions' as it is in there in which the calculations of the models for all techonologies are included. Other notebooks are important toi support that models and for a better undesrating of the figures or the information. However, those only use information from 2014, which makes them incomplete.

In this analysis we will use information related to:

    - Energetic Generation: extracted from Red Eléctrica de España (REE) website in several files (on a yearly basis)
    - Weather historical data: from AEMET API, it contains information related to the meteo stations of the whole country.
    - Provincias: As we want to take into consideration geo data but it is complicated to have all the detail, we are considering 'Provincias' as geo reference for the data we are analyzing.
    - Power Installed: It's a critical variable and it is provided also by REE in a monthly basis.
    - Weather_Predictions: It will be the 'food' for the model to work. The test data that we want to predict.
    - 'Embalses / Dams' Info: Critical for hidroelectric production.

