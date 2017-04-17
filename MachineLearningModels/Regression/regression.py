import pandas as pd
from decimal import *
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures
from sklearn import linear_model

def roundFunc(list):
    return [float(Decimal("%.2f" % e)) for e in list]

def model_prediction(train_df, test_df, countries):
    cols = ['top1', 'top2', 'top3']
    degrees = [1]
    for i in range(len(degrees)):
        polynomial_features = PolynomialFeatures(degree=degrees[i], include_bias=False)
        linear_regression = linear_model.LinearRegression()
        pipeline = Pipeline([("polynomial_features", polynomial_features),
                             ("linear_regression", linear_regression)])
        pipeline.fit(train_df[cols], train_df['gdp'])
        print countries, "\npredicted_gdp [2014 2015]:", roundFunc(pipeline.predict(test_df[cols])), \
            "\nactual_gdp [2014 2015]:", roundFunc(list(test_df['gdp'])), "\n"

def main():
    for countries in ['Canada', 'United States', 'Australia', 'United Kingdom', 'Norway', 'Sweden', 'Denmark', 'Netherlands',
                      'South Africa', 'Japan', 'France', 'Italy', 'Mexico', 'Spain', 'Greece', 'Finland', 'Germany',
                      'Belgium', 'Ireland', 'Portugal', 'India', 'Iceland', 'New Zealand', 'Switzerland', 'Israel',
                      'Brazil', 'China', 'Singapore', 'Russian Federation', 'Saudi Arabia']:
        filter_df = df[df['country'] == countries]
        train_df = filter_df[filter_df['year'] < 2014]
        test_df = filter_df[filter_df['year'] >= 2014]
        model_prediction(train_df, test_df, countries)


if __name__ == '__main__':
    path = '/home/hadoop/World-GDP-Analysis-And-Prediction/CleanedData'
    # ------------ Loading Data ------------------------- #
    df = pd.read_csv(path + '/top3_features_data_rfe.csv', sep=',')
    main()
