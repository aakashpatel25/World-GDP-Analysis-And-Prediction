# World-GDP-Analysis-And-Prediction
Clustering important economic factors that affect the GDP of a country and based on these indicators predict GDP of that country.

## Cleaning Additional indicator file

```cd DataCleaning/Programs```

```pwd``` 

/home/hadoop/World-GDP-Analysis-And-Prediction/DataCleaning/Programs

**Cleaning raw data files in ../rawDataFile ===>** *ALL_COMMODITY_EXPORT_DATA.txt, Crude_Oil_Export.txt, Crude_Oil_Import.txt, External_Debt_stock.txt, Natural_Gas-Export.txt, Natural_Gas-Import.txt, Net_barter_term_of_trade.txt, Time_require_to_start_a_business.txt*

`python clean_additional_ind.py`

## Feature Selection and Filling missing values

**Selecting top 30 countries for GDP analysis, plotting heatmap of pearson correlation between gdp indicators and gdp growth percentage, filltering top 10 indicators with highest correlation values for each country (**_../../CleanedData/most_relevent_features.csv_**), filling missing values, saving data to ===>** _../../CleanedData/filled.csv_

`python feature_selection.py`

![Alt text](corr.jpg?raw=true)


**Using Recursive Feature Elimination to choose top 3 features from top 10 indicators selected in previous step and performing MinMaxScaler Normalization**

`python top3_feature_selection.py`

**Top 3 features Name -** *../../CleanedData/top3_features_names_rfe.csv*

**Top 3 features Normalized Data -** *../../CleanedData/top3_features_data_rfe.csv*


## Training Linear Regression Model for each country corresponding to it's top3 features from previous step

`cd ../../MachineLearningModels/Regression/`

`python regression.py`

>Canada 
>predicted_gdp [2014 2015]: [2.74, 5.04] 
>actual_gdp [2014 2015]: [2.47, 1.08] 
>
>United States 
>predicted_gdp [2014 2015]: [1.22, 0.18] 
>actual_gdp [2014 2015]: [2.43, 2.43] 
>
>Australia 
>predicted_gdp [2014 2015]: [3.03, 2.88] 
>actual_gdp [2014 2015]: [2.5, 2.26] 
>
>Netherlands 
>predicted_gdp [2014 2015]: [0.22, 1.19] 
>actual_gdp [2014 2015]: [1.01, 1.99] 
>
>South Africa 
>predicted_gdp [2014 2015]: [2.67, 2.47] 
>actual_gdp [2014 2015]: [1.55, 1.28] 
>
>Italy 
>predicted_gdp [2014 2015]: [-2.09, -0.89] 
>actual_gdp [2014 2015]: [-0.34, 0.76] 
>
>India 
>predicted_gdp [2014 2015]: [6.47, 12.55] 
>actual_gdp [2014 2015]: [7.24, 7.57] 
>
>New Zealand 
>predicted_gdp [2014 2015]: [3.27, 3.79] 
>actual_gdp [2014 2015]: [3.17, 3.39] 
>
>Switzerland 
>predicted_gdp [2014 2015]: [1.92, -0.52] 
>actual_gdp [2014 2015]: [1.89, 0.91] 
>
>Israel 
>predicted_gdp [2014 2015]: [2.52, 2.85] 
>actual_gdp [2014 2015]: [2.6, 2.49] 
>
>China 
>predicted_gdp [2014 2015]: [8.05, 5.32] 
>actual_gdp [2014 2015]: [7.27, 6.9] 
>
>Singapore 
>predicted_gdp [2014 2015]: [4.99, 2.74] 
>actual_gdp [2014 2015]: [3.26, 2.01] 
>
>Saudi Arabia 
>predicted_gdp [2014 2015]: [5.75, 4.98] 
>actual_gdp [2014 2015]: [3.64, 3.49]

**Note: _Although our model is not highly accurate due to data limitation, Still it is able to capture genreal increment or decrement trend in GDP growth rate._** 


