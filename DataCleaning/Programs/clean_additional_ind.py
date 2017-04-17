import pandas as pd
import numpy as np

bool_import = []
bool_export = []

input_path = '/home/hadoop/World-GDP-Analysis-And-Prediction/DataCleaning/rawDataFile'
output_path = '/home/hadoop/World-GDP-Analysis-And-Prediction/DataCleaning/CleanedExcelFiles'


# ------------ Cleaning Commodity Data ------------------------- #
df = pd.read_csv(input_path + '/ALL_COMMODITY_EXPORT_DATA.txt', sep = ';')

for flow_type in df.Flow:
    if (flow_type.upper() == 'IMPORT' or flow_type.upper() == 'RE-IMPORT'):
        bool_import.append(True)
    else:
        bool_import.append(False)

    if (flow_type.upper() == 'EXPORT' or flow_type.upper() == 'RE-EXPORT'):
        bool_export.append(True)
    else:
        bool_export.append(False)

import_df = df[pd.Series(bool_import)].groupby(['Country or Area', 'Year'], as_index=False).sum().filter(items = ['Country or Area', 'Year', 'Trade (USD)'])
export_df = df[pd.Series(bool_export)].groupby(['Country or Area', 'Year'], as_index=False).sum().filter(items = ['Country or Area', 'Year', 'Trade (USD)'])

import_df.columns = ['country', 'year', 'trade_amount_USD']
export_df.columns = ['country', 'year', 'trade_amount_USD']

import_df.to_csv(output_path + '/commodity_import.csv', sep=';', index=False)
export_df.to_csv(output_path + '/commodity_export.csv', sep=';', index=False)

# ------------ Cleaning Crude Oil, External Debt Data ------------------------- #
import_df = pd.read_csv(input_path + '/Crude_Oil_Import.txt', sep = ';')
export_df = pd.read_csv(input_path + '/Crude_Oil_Export.txt', sep = ';')
external_debt_df = pd.read_csv(input_path + '/External_Debt_stock.txt', sep = ';')

import_df = import_df.filter(items = ['Country or Area', 'Year', 'Quantity']).dropna()
export_df = export_df.filter(items = ['Country or Area', 'Year', 'Quantity']).dropna()
external_debt_df = external_debt_df.filter(items = ['Country or Area', 'Year', 'Value']).dropna()

import_df[['Year']] = import_df[['Year']].apply(np.int64)
export_df[['Year']] = export_df[['Year']].apply(np.int64)
external_debt_df[['Year']] = external_debt_df[['Year']].apply(np.int64)

import_df.to_csv(output_path + '/crude_oil_import.csv', sep=';', index=False)
export_df.to_csv(output_path + '/crude_oil_export.csv', sep=';', index=False)
external_debt_df.to_csv(output_path + '/external_debt_stock.csv', sep=';', index=False)

# ------------ Cleaning Natural Gas Data ------------------------- #
import_df = pd.read_csv(input_path + '/Natural_Gas-Import.txt', sep = ';')
export_df = pd.read_csv(input_path + '/Natural_Gas-Export.txt', sep = ';')

import_df = import_df.filter(items = ['Country or Area', 'Year', 'Quantity']).dropna()
export_df = export_df.filter(items = ['Country or Area', 'Year', 'Quantity']).dropna()

import_df[['Year']] = import_df[['Year']].apply(np.int64)
export_df[['Year']] = export_df[['Year']].apply(np.int64)

import_df.to_csv(output_path + '/natural_gas_import.csv', sep=';', index=False)
export_df.to_csv(output_path + '/natural_gas_export.csv', sep=';', index=False)

# ------------ Cleaning Barter terms of Trade Data ------------------------- #
trade_df = pd.read_csv(input_path + '/Net_barter_term_of_trade.txt', sep = ';')
trade_df = trade_df.filter(items = ['Country or Area', 'Year', 'Value']).dropna()

trade_df[['Year']] = trade_df[['Year']].apply(np.int64)
trade_df.to_csv(output_path + '/net_barter_term_of_trade.csv', sep=';', index=False)

# ------------ Cleaning Time Required to start a business Data ------------------------- #
time_df = pd.read_csv(input_path + '/Time_require_to_start_a_business.txt', sep = ';')
time_df = time_df.filter(items = ['Country or Area', 'Year', 'Value']).dropna()

time_df[['Year', 'Value']] = time_df[['Year', 'Value']].apply(np.int64)
time_df.to_csv(output_path + '/time_to_start_new_business.csv', sep=';', index=False)
