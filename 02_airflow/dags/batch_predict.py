from datetime import datetime
from google.cloud import bigquery
import pandas as pd
import calendar
import pytz
import mlflow
import logging


def pull_temp_forecast(start_date, tz='America/Denver'):
    q = f"""
            WITH forecast_pit as
            (
            SELECT 
                forecast_time,
                MAX(creation_time) creation_time

            FROM
                `mlops-zoomcamp-354700.energy_data.weather_forecast`
            WHERE
                creation_time <= TIMESTAMP( "{start_date}", "{tz}")
                AND forecast_time >= TIMESTAMP("{start_date}", "{tz}")
            GROUP BY
                forecast_time
            )

            SELECT 
            f.forecast_time,
            f.temp_f,
            f.creation_time
            FROM `mlops-zoomcamp-354700.energy_data.weather_forecast` f
            INNER JOIN forecast_pit
            ON forecast_pit.creation_time =  f.creation_time 
            AND forecast_pit.forecast_time = f.forecast_time

            ORDER BY forecast_time
    """

    tz_info = pytz.timezone(tz)
    
    # interpolate temp_f forecast on hourly basis (previously it is only every 3 hours)
    df = (pd.read_gbq(q, project_id='mlops-zoomcamp-354700')
            .assign(temp_F=lambda df_: df_['temp_f'].astype(float))
            .set_index('forecast_time')
            .loc[:, 'temp_F']
            .resample('H')
            .interpolate('cubic')
            .reset_index()
            .assign(energy_timestamp_mtn=lambda df_: df_['forecast_time'].dt.tz_convert(tz_info))
            .drop(columns=['forecast_time'])
    )
    
    return df


def make_features(df, features):
    df_out = (df
                .reset_index()
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


def save_results(df, y_pred, run_id, start_date, tz, output_file):
    df_result = (pd.DataFrame()
                    .assign(energy_timestamp_mtn=df.index,
                            predicted_energy_demand=y_pred,
                            temp_f_forecast=df['temp_F'].reset_index(drop=True),
                            model_version=run_id,
                            prediction_start_date=start_date,
                            prediction_creation_date=datetime.now())
                )
    logging.info(f'df_result_prediction_start_date: {df_result.prediction_start_date}')

    df_result['prediction_start_date'] = df_result['prediction_start_date'].dt.tz_localize(tz)

    logging.info(f'df_result_prediction_start_date: {df_result.prediction_start_date}')
    df_result.to_parquet(output_file, index=False)


def apply_model(run_id, features, start_date, date_fmt, output_file, tz='America/Denver'):
    logging.info(f'start_date: {start_date}')
    logging.info(f'date_fmt: {date_fmt}')

    start_date = datetime.strptime(start_date, date_fmt)
    logging.info('converted start_date from string to datetime')
    logging.info(f'start_date: {start_date}')

    df = pull_temp_forecast(start_date, tz)
    logging.info(f'df: {df.energy_timestamp_mtn.min()}')
    logging.info(f'df: {df.energy_timestamp_mtn.max()}')

    df = make_features(df, features=features)
    model = load_model(run_id)
    y_pred = model.predict(df)
    save_results(df, y_pred, run_id, start_date, tz, output_file)
    return output_file


if __name__ == '__main__':

    start_date = datetime(2022, 7, 15, 0, 0, 0)
    tz = "US/Mountain"
    run_id = '49c833f911ae43488e67063f410b7b5e'
    output_file = 'output.parquet'

    features = ['temp_F', 'year', 'day_of_year', 'hour', 'is_weekend', 
            'is_summer', 'month', 'temp_F_squared', 'hour_squared', 'hour_cubed']
    
    apply_model(run_id, features, start_date, output_file, tz)

    result = pd.read_parquet(output_file)
    print(result)
    