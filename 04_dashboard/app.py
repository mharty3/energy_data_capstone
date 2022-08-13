import os
from datetime import date, timedelta

from info import info_text, note
from google.oauth2 import service_account
from google.cloud import bigquery

import pandas as pd
import pandas_gbq
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st

# https://docs.streamlit.io/knowledge-base/tutorials/databases/bigquery#enable-the-bigquery-api
# https://pandas-gbq.readthedocs.io/en/latest/howto/authentication.html

st.set_page_config(layout="wide", initial_sidebar_state='expanded')

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mlops-zoomcamp-354700")
BIGQUERY_DATASET = 'energy_data_prod'
MODEL_VERSION = "49c833f911ae43488e67063f410b7b5e"


# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    # query_job = client.query(query)
    # rows_raw = query_job.result()
    # # Convert to list of dicts. Required for st.experimental_memo to hash the return value.
    # rows = [dict(row) for row in rows_raw]
    return pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)


def plot_demand_time_series(eia_forecast_demand, prod_model_demand, actual_demand, weather_2022):

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True)

    fig.add_traces(
        list(px.line(
            eia_forecast_demand, 
            x='timestamp_MTN', 
            y='value',
            title="Actual and Forecasted Electrical Demand, Xcel Energy, Colorado",
            labels={'value': 'EIA Forecasted Demand (megawatthours)'}
            )
            .select_traces()
        )
    )

    fig.add_traces(
        list(px.line(actual_demand, x='timestamp_MTN', y='value', labels={'value': 'Actual_Demand (megawatthours)'}).select_traces())
        )

    fig.add_traces(
      list(px.line(prod_model_demand, x='timestamp_MTN', y='predicted_energy_demand', labels={'value': 'Predicted_Demand (megawatthours)'}).select_traces())
      )

    fig.add_trace(
        list(px.scatter(weather_2022, x='observation_time_MTN', y='temp_F').select_traces())[0],
        row=2, col=1
        )

    fig['data'][1]['line']['color']='#ef476f'
    fig['data'][1]['line']['width']=5
    fig['data'][0]['line']['color']='#06d6a0'
    fig['data'][0]['line']['width']=2

    fig['data'][1]['showlegend']=True
    fig['data'][1]['name']='Actual Demand (MWh)'
    fig['data'][0]['showlegend']=True
    fig['data'][0]['name']='EIA Demand Forecast (MWh)'
    fig['data'][2]['name']='Modeled Demand (MWh)'
    fig['data'][2]['showlegend']=True

    fig['data'][3]['showlegend']=True
    fig['data'][3]['name']='Denver Airport Actual Temperature (F)'

    return fig

def main():
    TODAY = date.today()
    TOMORROW = TODAY + timedelta(2)
    WEEK_PRIOR = TODAY - timedelta(7)
    with st.form('date_picker'):
        start_date, end_date = st.date_input('Select a date range, then click "Update"', min_value=date(2015,7, 4), max_value=TOMORROW, value=(WEEK_PRIOR, TOMORROW))
        submitted = st.form_submit_button("Update")


    actual_demand = run_query(f"""SELECT * 
                                  FROM 
                                    `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_eia_demand_historical` 
                                  WHERE 
                                    date(timestamp_MTN) BETWEEN date('{start_date}') and date('{end_date}') 
                                  ORDER BY timestamp_MTN""")

    eia_forecast_demand = run_query(f"""SELECT * 
                                        FROM 
                                          `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_eia_demand_forecast` 
                                        WHERE 
                                          date(timestamp_MTN) BETWEEN date('{start_date}') and date('{end_date}')
                                        ORDER BY timestamp_MTN""")

    prod_model_demand = run_query(f"""SELECT
                                        DATETIME(energy_timestamp_mtn, "America/Denver") as timestamp_MTN,
                                        predicted_energy_demand,
                                        temp_f_forecast,
                                        model_version,
        
                                    FROM `{PROJECT_ID}.energy_data.energy_demand_forecasts`                              
                                      WHERE DATETIME_DIFF(DATETIME(energy_timestamp_mtn, "America/Denver"), DATETIME(prediction_start_date, "America/Denver"), HOUR) < 24
                                      and 
                                      model_version = '{MODEL_VERSION}'
                                      and 
                                      DATETIME(energy_timestamp_mtn, "America/Denver") BETWEEN date('{start_date}') and date('{end_date}')
                                      ORDER BY energy_timestamp_mtn"""
                                      )                                        


    
    weather_2022 = run_query(f"""SELECT * 
                               FROM 
                                 `{PROJECT_ID}.{BIGQUERY_DATASET}.recorded_temperature` 
                               WHERE 
                                 observation_time_MTN BETWEEN date('{start_date}') and date('{end_date}')
                               ORDER BY observation_time_MTN""")

    fig = plot_demand_time_series(eia_forecast_demand, prod_model_demand, actual_demand, weather_2022)
    fig.update_layout(height=700)
    st.plotly_chart(fig, use_container_width=True)

if __name__ == '__main__':

    st.title('⚡ Energy Demand and Temperature for Xcel Energy in CO ⚡')
    st.sidebar.write(info_text)
    # with st.sidebar.expander('Note on missing data between May 20 and May 30, 2022:'):
    #   st.write(note)

    main()

