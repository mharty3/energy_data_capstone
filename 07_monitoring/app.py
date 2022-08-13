from google.oauth2 import service_account
from google.cloud import bigquery
import probscale
import numpy as np
import pandas as pd
import calendar
import hvplot.pandas
import holoviews as hv
from datetime import date, timedelta
import os
import streamlit as st

st.set_page_config(layout='wide')

# Query the data from BigQuery.
# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mlops-zoomcamp-354700")
BIGQUERY_DATASET = 'energy_data_prod'

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    # query_job = client.query(query)
    # rows_raw = query_job.result()
    # # Convert to list of dicts. Required for st.experimental_memo to hash the return value.
    # rows = [dict(row) for row in rows_raw]
    return pd.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)


# model selection
q = f"""SELECT DISTINCT(model_version) 
       FROM `mlops-zoomcamp-354700.energy_data_prod.ml_model_metrics` 
       """
model_options = run_query(q)['model_version'].tolist()
model_selection = st.sidebar.selectbox("Select Model", model_options)


# date range 
TODAY = date.today()
TOMORROW = TODAY + timedelta(2)
TWO_WEEK_PRIOR = TODAY - timedelta(14)
with st.sidebar.form('date_picker'):
        start_date, end_date = st.date_input('Select a date range, then click "Update"', min_value=date(2015,7, 4), max_value=TOMORROW, value=(TWO_WEEK_PRIOR, TOMORROW))
        submitted = st.form_submit_button("Update")

q = f"""SELECT * 
       FROM `mlops-zoomcamp-354700.energy_data_prod.ml_model_metrics`
       WHERE hours_from_pred_start <= 24 and 
             date(energy_timestamp_mtn) BETWEEN date('{start_date}') and date('{end_date}') 
       """
metrics = run_query(q)

st.title("Model Monitoring Dashboard")
st.write(f"Analyzing predictions between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}.\n Use the sidebar to select a different date range.")

# display key metrics
st.write('## Hourly Energy Demand Prediction Metrics')
col1, col2, col3 = st.columns(3)
col1.metric('Mean Error', f"{round(metrics['energy_error'].mean(), 2)} MWh")
col2.metric('Mean Absolute Error', f"{round(metrics['energy_error_abs'].mean(), 2)} MWh")
col3.metric('Mean Absolute Percentage Error', f"{round(metrics['energy_error_abs_pct'].mean() * 100, 2)}%")

st.write('## Hourly Temperature Forecast Metrics')
col1, col2, col3 = st.columns(3)
col1.metric('Mean Error', f"{round(metrics['temp_f_error'].mean(), 2)} degF")
col2.metric('Mean Absolute Error', f"{round(metrics['temp_f_error_abs'].mean(), 2)} degF")
col3.metric('Mean Absolute Percentage Error', f"{round(metrics['temp_f_error_abs_pct'].mean() * 100, 2)}%")

st.markdown("""---""") 
st.write('## Hourly Monitoring Plots')
# plot actual vs predicted scatter plot
st.write('### Actual vs Predicted')
e = metrics.hvplot.scatter(x='energy_demand', y='predicted_energy_demand', label='Energy Demand (MWh)')
t = metrics.hvplot.scatter(x='temp_F', y='temp_f_forecast', label='Temp (F)')

st.bokeh_chart(
    hv.render(e + t)
)

# plot actual vs predicted over time
st.write('### Actual vs Predicted Over Time')
actual = metrics.hvplot(x='energy_timestamp_mtn', y='energy_demand', label='actual')
predicted = metrics.hvplot(x='energy_timestamp_mtn', y='predicted_energy_demand', label='predicted', title='Energy Demand (MWh) - Actual vs Predicted Over Time')
e = actual * predicted

actual = metrics.hvplot(x='energy_timestamp_mtn', y='temp_F', label='actual')
predicted = metrics.hvplot(x='energy_timestamp_mtn', y='temp_f_forecast', label='predicted', title='Temp (F) - Actual vs Predicted Over Time')
t = actual * predicted

st.bokeh_chart(
    hv.render(e + t)
)

# plot error over time
st.write('### Error Over Time')
actual = metrics.hvplot(x='energy_timestamp_mtn', y='energy_error', label='Energy Demand (MWh) Error (actual - predicted) Over Time')
e = actual * hv.HLine(0).opts(color='gray', line_width=1)

actual = metrics.hvplot(x='energy_timestamp_mtn', y='temp_f_error', label='Temp (F) Error (actual - predicted) Over Time')
t = actual * hv.HLine(0).opts(color='gray', line_width=1)
st.bokeh_chart(
    hv.render(e + t)
)

# plot absolute percentage error over time
st.write('### Absolute Percentage Error Over Time')
actual = metrics.hvplot(x='energy_timestamp_mtn', y='energy_error_abs_pct', label='Energy Demand (MWh) Absolute Percentage Error')
e = actual

actual = metrics.hvplot(x='energy_timestamp_mtn', y='temp_f_error_abs_pct', label='Temp (F) Absolute Percentage Error')
t = actual
st.bokeh_chart(
    hv.render(e + t)
)

# plot error distribution
st.write('### Error Distribution')
e = metrics.hvplot.hist('energy_error', label='Energy Demand (MWh) Error Distribution')
t = metrics.hvplot.hist('temp_f_error', label='Temp (F) Error Distribution')
st.bokeh_chart(
    hv.render(e + t)
)
