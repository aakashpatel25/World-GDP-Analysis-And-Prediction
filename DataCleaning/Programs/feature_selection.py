import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

path = '/home/hadoop/World-GDP-Analysis-And-Prediction/CleanedData'

# ------------------------- Cleaned Data File ------------------------- #
df = pd.read_csv(path + '/gdpCleaned.csv', sep=',')

df_count = df.groupby(['country'], as_index=False).count()

sum = df_count['debt'] + df_count['inflation'] + df_count['unemployment'] + df_count['lendingInterest'] + df_count['techExport'] + \
      df_count['totalReserves'] + df_count['fdi'] + df_count['cropProduction'] + df_count['oilPrice'] + df_count['gasPrice'] + \
      df_count['workingPop'] + df_count['exchangeRate'] + df_count['oilProd'] + df_count['oilImportPrice'] + df_count['ppp'] + \
      df_count['researcherPer1k'] + df_count['ppi'] + df_count['totalIndProd'] + df_count['sharePrice'] + df_count['pollution'] + \
      df_count['narrowMoneyM1'] + df_count['broadMoney'] + df_count['longTermInterest'] + df_count['shortTermInterest'] + \
      df_count['importExportRatio'] + df_count['newBusinessDiff'] + df_count['naturalGasImport'] + df_count['naturalGasExport'] + \
      df_count['externalDebtStock'] + df_count['oilImport'] + df_count['oilExport'] + df_count['tradeImport'] + df_count['tradeExport']

df_count['total_count'] = sum

print '------------------------- Top 30 countries with most data available -------------------------\n'
print df_count[['country', 'total_count']].sort_values(by='total_count', ascending=False).head(30)

country_list = ['Canada', 'United States', 'Australia', 'United Kingdom', 'Norway', 'Sweden', 'Denmark', 'Netherlands',
                'South Africa', 'Japan', 'France', 'Italy', 'Mexico', 'Spain', 'Greece', 'Finland', 'Germany','Belgium',
                'Ireland', 'Portugal', 'India', 'Iceland', 'New Zealand', 'Switzerland', 'Israel', 'Brazil', 'China',
                'Singapore', 'Russian Federation', 'Saudi Arabia']

print '\nList of 30 counties of interest for GDP prediction\n'
print country_list

bool_country = []

for country in df['country']:
    if country in country_list:
        bool_country.append(True)
    else:
        bool_country.append(False)

filter_df = df[bool_country]
del filter_df['gdpNote']

filter_df_copy = filter_df.copy()[0:0]
filter_df_copy = filter_df_copy.drop(['countryName', 'countryCode', 'year'], axis=1)

for countries in country_list:
    series = filter_df[filter_df['country'] == countries].select_dtypes(include=['float64']).corr().loc['gdp', :]
    series = series.append(pd.Series(countries, index=['country']))
    DF = pd.DataFrame([series], columns=series.index)
    filter_df_copy = filter_df_copy.append(DF)

indexed_df = filter_df_copy.set_index(filter_df_copy['country']).select_dtypes(include=['float64'])
del indexed_df['gdp']
cols = ['debt', 'inflation', 'unemployment', 'lendingInterest', 'techExport', 'totalReserves', 'fdi', 'cropProduction',
        'oilPrice', 'gasPrice', 'workingPop', 'exchangeRate', 'oilProd', 'oilImportPrice', 'ppp', 'researcherPer1k', 'ppi',
        'totalIndProd', 'sharePrice', 'pollution', 'narrowMoneyM1', 'broadMoney', 'longTermInterest',
        'shortTermInterest', 'importExportRatio', 'newBusinessDiff', 'naturalGasImport', 'naturalGasExport', 'externalDebtStock',
        'oilImport', 'oilExport', 'tradeImport', 'tradeExport']

indexed_df = indexed_df[cols]

# Plotting correlation values heatmap

f, ax = plt.subplots(figsize=(11, 9))
plt.title('Correlation of Indicator vs GDP growth rate')
cmap = sns.diverging_palette(220, 10, as_cmap=True)
sns.heatmap(indexed_df, cmap=cmap, vmax=1, square=True, linewidths=.5, cbar_kws={"shrink": .5}, ax=ax)
labels = ax.get_xticklabels()
plt.setp(labels, rotation=90)
labels = ax.get_yticklabels()
plt.setp(labels, rotation=0)
plt.xlabel("Indicators")
plt.ylabel("Country")
plt.subplots_adjust(bottom=0.15)
plt.subplots_adjust(left=0.15)
plt.show()

# Filtering top 10 features with highest correlation values for each country

indexed_df = abs(indexed_df)
nlargest = 10
order = np.argsort(-indexed_df.values, axis=1)[:, :nlargest]
result = pd.DataFrame(indexed_df.columns[order], columns=['top{}'.format(i) for i in range(1, nlargest + 1)], index=indexed_df.index)
result.to_csv(path + '/most_relevent_features.csv', sep=';')

# Filling missing values and normalizing data

def fill_missing_val(df):
    df.columns = ['top1', 'top2', 'top3', 'top4', 'top5', 'top6', 'top7', 'top8', 'top9', 'top10', 'year', 'gdp', 'country']
    avg_df = df[df['year'] >= 1985]
    avg_df = avg_df[avg_df['year'] <= 2010]
    max_df = df[df['year'] > 2010]
    for col in ['top1', 'top2', 'top3', 'top4', 'top5', 'top6', 'top7', 'top8', 'top9', 'top10']:
        max_df[col] = max_df[col].fillna(float(df.describe()[col].loc[['max']]))
        avg_df[col] = avg_df[col].fillna(float(df.describe()[col].loc[['mean']]))
    frames = [avg_df, max_df]
    result = pd.concat(frames)
    return result

result['country'] = result.index
feature_list_df = result

filled_df = []
for countries in country_list:
    pred_df = filter_df[filter_df['country'] == countries]
    col_list = list(feature_list_df[feature_list_df['country'] == countries].iloc[0])[0:nlargest]
    col_list.append('year')
    col_list.append('gdp')
    col_list.append('country')
    filled_df.append(fill_missing_val(pred_df[col_list]))

filled_dataframe = pd.concat(filled_df)
filled_dataframe = filled_dataframe.fillna(0.0)

# Saving filled missing value file
filled_dataframe.to_csv(path + '/filled.csv', sep=';', index=False)

print '\n ---------------- Finished !!! ----------------'
