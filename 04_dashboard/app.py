import os
from datetime import date, timedelta

from google.oauth2 import service_account
from google.cloud import bigquery

import pandas as pd
import pandas_gbq
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st

# https://docs.streamlit.io/knowledge-base/tutorials/databases/bigquery#enable-the-bigquery-api
# https://pandas-gbq.readthedocs.io/en/latest/howto/authentication.html

st.set_page_config(layout="wide")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "data-eng-zoomcamp-339102")
BIGQUERY_DATASET = 'energy_data'


# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    # query_job = client.query(query)
    # rows_raw = query_job.result()
    # # Convert to list of dicts. Required for st.experimental_memo to hash the return value.
    # rows = [dict(row) for row in rows_raw]
    return pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)


def plot_demand_time_series(forecast_demand, actual_demand, weather_2022):

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True)

    fig.add_traces(
        list(px.line(
            forecast_demand, 
            x='timestamp', 
            y='value',
            title="Actual and Forecasted Electrical Demand, Xcel Energy, CO",
            labels={'value': 'Demand (megawatthours)'}
            )
            .select_traces()
        )
    )

    fig.add_traces(
        list(px.line(actual_demand, x='timestamp', y='value', labels={'value': 'Actual_Demand (megawatthours)'}).select_traces())
        )

    fig.add_trace(
        list(px.scatter(weather_2022, x='DATE', y='temperature_degC').select_traces())[0],
        row=2, col=1
        )

    fig['data'][1]['line']['color']='#ef476f'
    fig['data'][1]['line']['width']=5
    fig['data'][0]['line']['color']='#06d6a0'
    fig['data'][0]['line']['width']=2

    fig['data'][1]['showlegend']=True
    fig['data'][1]['name']='Actual Demand'
    fig['data'][0]['showlegend']=True
    fig['data'][0]['name']='EIA Demand Forecast'

    return fig

def main():
    TODAY = date.today()
    TOMORROW = TODAY + timedelta(2)
    WEEK_PRIOR = TODAY - timedelta(7)
    with st.form('date_picker'):
        start_date, end_date = st.date_input('Select Data Date Range', value=(WEEK_PRIOR, TOMORROW))
        submitted = st.form_submit_button("Update")


    actual_demand = run_query(f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_eia_demand_historical` WHERE date(timestamp) BETWEEN date('{start_date}') and date('{end_date}')")
    forecast_demand = run_query(f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_eia_demand_forecast` WHERE date(timestamp) BETWEEN date('{start_date}') and date('{end_date}')")
    
    weather_2022 = run_query(f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.2022_weather_station_native` WHERE date(DATE) BETWEEN date('{start_date}') and date('{end_date}')")

    fig = plot_demand_time_series(forecast_demand, actual_demand, weather_2022)
    st.plotly_chart(fig, use_container_width=True)

if __name__ == '__main__':
    main()