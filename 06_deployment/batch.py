#!/usr/bin/env python
# coding: utf-8

import os
import sys

import calendar
from datetime import datetime

import pandas as pd

import mlflow



def pull_training_data(data_start, data_end):
    q = "SELECT * FROM `mlops-zoomcamp-354700.energy_data_prod.joined_temp_and_demand`"
    df_raw = pd.read_gbq(q, project_id='mlops-zoomcamp-354700')
    return df_raw[df_raw['energy_timestamp_mtn'].between(data_start, data_end)]
    

def trim_data(df, min_val=2000, max_val=11000):
    return df[df['energy_demand'].between(min_val, max_val)].set_index('energy_timestamp_mtn')


def make_features(df, min_y_val, max_y_val, features):
    df_out = (trim_data(df, min_y_val, max_y_val)
                                     .reset_index()
                                     .dropna(subset=['energy_demand', 'temp_F'])
                                     .assign(
                                            year=lambda df_: df_['energy_timestamp_mtn'].dt.year,
                                            day_of_year=lambda df_: df_['energy_timestamp_mtn'].dt.day_of_year,
                                            hour=lambda df_: df_['energy_timestamp_mtn'].dt.hour,
                                            is_weekend=lambda df_: df_['energy_timestamp_mtn'].dt.day_of_week >= 5, # saturady day_of_week = 5, sunday = 6
                                            is_summer=lambda df_: df_['energy_timestamp_mtn'].dt.month.between(5, 9, inclusive='both'),
                                            month=lambda df_: df_['energy_timestamp_mtn'].dt.month,
                                            temp_F_squared=lambda df_: df_['temp_F'] * df_['temp_F'],
                                            hour_squared=lambda df_: df_['hour'] ** 2,
                                            hour_cubed=lambda df_: df_['hour'] ** 3,
                                    )

                                .set_index('energy_timestamp_mtn')                                    
            )


    for month in calendar.month_name[1:]:
        df_out[month] = pd.to_numeric(df_out.index.month_name == month)
        
    return df_out[features]



def load_model(run_id):
    logged_model = f'gs://mlflow-runs-mlops-zoomcamp-354700/2/{run_id}/artifacts/model'
    model = mlflow.pyfunc.load_model(logged_model)
    return model


def save_results(df, y_pred, run_id, output_file):
    df_result = (pd.DataFrame()
                    .assign(energy_timestamp_mtn=df.index,
                            predicted_energy_demand=y_pred,
                            model_version=run_id,
                            prediction_date=datetime.now())
                )
    df_result.to_parquet(output_file, index=False)


def apply_model(run_id, features, data_start, data_end, output_file):
    df = pull_training_data(data_start, data_end)
    df = make_features(df, min_y_val=0, max_y_val=100, features=features)
    model = load_model(run_id)
    y_pred = model.predict(df)
    save_results(df, y_pred, run_id, output_file)
    return output_file


if __name__ == '__main__':
    data_start = '2019-01-01'
    data_end = '2019-12-31'
    output_file = 'output.parquet'
    run_id = '49c833f911ae43488e67063f410b7b5e'
    features = ['temp_F', 'year', 'day_of_year', 'hour', 'is_weekend', 
                'is_summer', 'month', 'temp_F_squared', 'hour_squared', 'hour_cubed']
    apply_model(run_id, features, data_start, data_end, output_file)
    print(f'Predictions saved to {output_file}')