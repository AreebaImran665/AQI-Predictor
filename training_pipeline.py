# -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------
# ------------------------------------  Feature Script 2   --------------------------------------------
# -------------------------------   Model Training Pipeline  ------------------------------------------
# -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------

import hopsworks

# Fetching Data From Hopswork
project = hopsworks.login(api_key_value="n7jRofG3Y9HUQ8Zi.hUbA78pZislL2kOnmPCnOPYberwqZf798dkc1ebR1czoVZ3LwMYsPvKonujAjQkY") 

fs = project.get_feature_store()  # Access the feature store

# Get the Feature Group
feature_group = fs.get_feature_group(name="weather_and_pollutant_data", version=1)

# Fetch the data from the Feature Group
data = feature_group.read()

# Check its type
print(type(data))  # Ensure whether it's Pandas


# -----------------------------------------------------------------------------------------------------------
# ------------------------------------------  Model Training  -----------------------------------------------
# -----------------------------------------------------------------------------------------------------------


import pandas as pd
import numpy as np

data['precipitation'].fillna(0, inplace=True)

df = data.drop(['readable_time', 'day_offset', 'latitude', 'longitude'], axis=1)

import warnings
warnings.filterwarnings('ignore')

# features (x) and target (y)
features = df.drop(df.columns[3], axis=1)
target = df.iloc[:, 3]

# Feature Scaling
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
x = scaler.fit_transform(features)

y = target

# Features Importance
from sklearn.ensemble import ExtraTreesRegressor
model = ExtraTreesRegressor()
model.fit(x,y)
print(model.feature_importances_)

# Train Test Split
from sklearn.model_selection import train_test_split
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, train_size=0.8, random_state=42)

# Random Forest
from sklearn.ensemble import RandomForestRegressor
rf=RandomForestRegressor()
rf.fit(x_train,y_train)

# Model Evaluation
print("R-Squared on train set: {}".format(rf.score(x_train, y_train)))

from sklearn.model_selection import cross_val_score
score=cross_val_score(rf,x,y,cv=5)
score.mean()

# Hyper Parameter Tuning
from sklearn.model_selection import RandomizedSearchCV

n_estimators = [int(x) for x in np.linspace(start = 100, stop = 1200, num = 12)]
print(n_estimators)

# Number of trees in random forest
n_estimators = [int(x) for x in np.linspace(start = 100, stop = 1200, num = 12)]

# Number of features to consider at every split
max_features = ['auto', 'sqrt']

# Maximum number of levels in tree
max_depth = [int(x) for x in np.linspace(5, 30, num = 6)]

# Minimum number of samples required to split a node
min_samples_split = [2, 5, 10, 15, 100]

# Minimum number of samples required at each leaf node
min_samples_leaf = [1, 2, 5, 10]

# Create the random grid
random_grid = {'n_estimators': n_estimators,
               'max_features': max_features,
               'max_depth': max_depth,
               'min_samples_split': min_samples_split,
               'min_samples_leaf': min_samples_leaf}

print(random_grid)
print(n_estimators)

# Random search of parameters, using 3 fold cross validation, search across 100 different combinations
rf_random = RandomizedSearchCV(estimator = rf, param_distributions = random_grid,scoring='neg_mean_squared_error', n_iter = 10, cv = 5, verbose=2, random_state=42, n_jobs = 1)
rf_random.fit(x_train,y_train)
print(rf_random.best_score_)

# Evaluation Metrics

rf_prediction=rf.predict(x_test)

from sklearn import metrics
print('MAE:', metrics.mean_absolute_error(y_test, rf_prediction))
print('MSE:', metrics.mean_squared_error(y_test, rf_prediction))
print('RMSE:', np.sqrt(metrics.mean_squared_error(y_test, rf_prediction)))
print('R^2:', metrics.r2_score(y_test, rf_prediction))

# Example for saving a trained scikit-learn model
import joblib
joblib.dump(rf, "rf_model.pkl")

# Access the Model Registry
mr = project.get_model_registry()

# Register your model in the Model Registry
model_name = "random_forest"  
description = "Random Forest Model for predicting AQI." 

# Create a new model entry
model = mr.python.create_model(
        name=model_name,
        description=description
)

# Save the model file to the Model Registry
model_path = "rf_model.pkl" 
model.save(model_path)