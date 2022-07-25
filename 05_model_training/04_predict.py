from datetime import datetime
from google.cloud import bigquery
import pandas as pd


def pull_temp_forecast(current_date,):
    q = f"""
            WITH forecast_pit as
            (
            SELECT 
                forecast_time,
                MAX(creation_time) creation_time

            FROM
                `mlops-zoomcamp-354700.energy_data.weather_forecast`
            WHERE
                creation_time < TIMESTAMP( "{current_date}")
                AND forecast_time > TIMESTAMP("{current_date}")
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
    
    # interpolate temp_f forecast on hourly basis (previously it is only every 3 hours)
    df = (pd.read_gbq(q, project_id='mlops-zoomcamp-354700')
            .assign(temp_f=lambda df_: df_['temp_f'].astype(float))
            .set_index('forecast_time')
            .loc[:, 'temp_f']
            .resample('H')
            .interpolate('cubic')
            .reset_index()
    )
    
    return df


if __name__ == '__main__':
    current_date = datetime(2022, 7, 15, 0, 0, 0)
    df_forecast = pull_temp_forecast(current_date)
    print(df_forecast)