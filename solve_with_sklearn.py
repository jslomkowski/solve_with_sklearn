"""
in this demo, I would like to show how you can solve business problems not only 
by using machine learning tools but also by more conventional algorithms like 
scipy optimize.
"""

import numpy as np
import pandas as pd
from scipy import optimize
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import OrdinalEncoder
# from category_encoders import OrdinalEncoder
from tpot import TPOTRegressor

# import category_encoders as ce


def mape(y, y_pred, weight=1):
    """Custom metric with bias possibilities. Weight parameter will multiply
    every prediction by it's value thus changing the mape error.

    Arguments:
        y {array} -- ground truth
        y_pred {array} -- prediction made by estimator

    Keyword Arguments:
        weight {int} -- custom bias for y_pred (default: {1})

    Returns:
        [float] -- mape error with added bias (default = 1)
    """
    mape = round(mean_absolute_error(y, y_pred*weight) / np.mean(y), 4)
    return mape


# def objective(x):
#     return mean_absolute_error(df['SalePrice'], df['y_pred_lp'] * x)


def func_error(val, values, df):
    """Error function to be passed to scipy.optimize

    Arguments:
        val {column values} -- distinct values from each of the columns
        values {column values} -- distinct values from each of the columns
        df {DataFrame} -- DateFrame with values

    Returns:
        [type] -- [description]
    """
    assert len(val) == len(values)
    lookup = dict()
    for i in range(len(val)):
        lookup[values[i]] = val[i]
    df = df.replace(lookup)
    error = np.abs(df['SalePrice'] -
                   np.multiply(df['LowQualFinSF'], df['Functional']))
    return np.mean(error) / np.mean(df['SalePrice'])


# Let's load data
df_org = pd.read_csv('datasets/ames.csv')

# Linear programming
# Load the data
df = df_org[['LowQualFinSF', 'Functional', 'SalePrice']]

# Let's concatenate unique values per selected columns for which later we will
# create weights
values = np.concatenate([df['LowQualFinSF'].unique(),
                         df['Functional'].unique()])

# set initial guess (from this level minimize will start it's search)
initial = np.repeat(np.sqrt(df['SalePrice'].mean()), values.size)

# optimize our problem
res = optimize.minimize(func_error,
                        args=(values, df),
                        x0=initial,
                        method='Nelder-Mead',
                        options={'maxiter': 2})

# result is the dataframe with weights that need to be multiplied in order to
# solve our problem
res_df = pd.DataFrame({'res': values, 'min_val': res.x})

# lets add our weights to dataframe...
for col in ['LowQualFinSF', 'Functional']:
    df[col] = df[col].map(res_df.set_index('res')['min_val'])

# ...and create prediction
df['y_pred_lp'] = df['LowQualFinSF'] * df['Functional']

# calculate mape error
print('mape on linear_programming', mape(df['SalePrice'], df['y_pred_lp']))

# TPOT time
X = df_org[['LowQualFinSF', 'Functional']]
y = df['SalePrice']
X = OrdinalEncoder().fit_transform(X)
# this will take a while if you launch below code
model = TPOTRegressor(n_jobs=-1, verbosity=1, config_dict="TPOT light")
model.fit(X, y)
y_pred_TPOT = model.predict(X)
df['y_pred_TPOT'] = y_pred_TPOT
print('mape on TPOT', mape(df['SalePrice'], df['y_pred_TPOT']))

# RESULTS
# mape on linear_programming 30.09
# mape on TPOT 30.95

# Linear programming can handle optimization tasks just as good as machine
# learning or even better. It can act as stand alone algorithm or as optimizer
# for other machine learning problem
