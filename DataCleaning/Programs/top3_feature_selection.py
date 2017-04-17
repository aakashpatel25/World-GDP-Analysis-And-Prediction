import pandas as pd
from sklearn.feature_selection import RFE
from sklearn.svm import SVR
from sklearn.preprocessing import MinMaxScaler


path = '/home/hadoop/World-GDP-Analysis-And-Prediction/CleanedData'

df = pd.read_csv(path + '/filled.csv', sep=';')
features_df = pd.read_csv(path + '/most_relevent_features.csv', sep=';')


df_list = []
top_3_feature_list = []

# Normalizing data and choosing top 3 most relevant features for each country using recursive feature elimination
for countries in ['Canada', 'United States', 'Australia', 'United Kingdom', 'Norway', 'Sweden', 'Denmark', 'Netherlands',
                   'South Africa', 'Japan', 'France', 'Italy', 'Mexico', 'Spain', 'Greece', 'Finland', 'Germany', 'Belgium',
                   'Ireland', 'Portugal', 'India', 'Iceland', 'New Zealand', 'Switzerland', 'Israel', 'Brazil', 'China',
                   'Singapore', 'Russian Federation', 'Saudi Arabia']:
    country_df = df[df['country'] == countries]
    country_features_df = features_df[features_df['country'] == countries]
    feature_col = ['top1', 'top2', 'top3', 'top4', 'top5', 'top6', 'top7', 'top8', 'top9', 'top10']
    train_df = country_df[country_df['year'] < 2014].reset_index()
    test_df = country_df[country_df['year'] >= 2014].reset_index()

    norm = MinMaxScaler(feature_range=(0, 1)).fit(train_df[feature_col])
    estimator = SVR(kernel="linear")
    selector = RFE(estimator, 3, step=1)
    selector = selector.fit(norm.transform(train_df[feature_col]), train_df['gdp'])

    tmp = [country_features_df[feature_col[i]].values[0] for i in selector.support_.nonzero()[0]]
    tmp.append(countries)
    top_3_feature_list.append(tmp)
    trans_train_df = pd.DataFrame(selector.transform(norm.transform(train_df[feature_col]))).join(train_df[['year', 'gdp', 'country']])
    trans_test_df = pd.DataFrame(selector.transform(norm.transform(test_df[feature_col]))).join(test_df[['year', 'gdp', 'country']])
    trans_train_df.columns = ['top1', 'top2', 'top3', 'year', 'gdp', 'country']
    trans_test_df.columns = ['top1', 'top2', 'top3', 'year', 'gdp', 'country']
    df_list.append(trans_train_df)
    df_list.append(trans_test_df)
top_3_feature_df = pd.DataFrame(top_3_feature_list)
top_3_feature_df.columns = ['top1', 'top2', 'top3', 'country']
top_3_feature_df.to_csv(path + '/top3_features_names_rfe.csv', sep=',', index=False)
pd.concat(df_list).to_csv(path + '/top3_features_data_rfe.csv', sep=',', index=False)
