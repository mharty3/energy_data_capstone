# Data Engineering Zoomcamp Capstone Project

This is the repo for my capstone project for the Data Engineering Zoomcamp from Data Talks Club. All of my course work and notes for the zoomcamp are in [this other repo](https://github.com/mharty3/data_engineering_zoomcamp_2022).

[Link to live dashboard](https://share.streamlit.io/mharty3/energy_data_exploration/04_dashboard/app.py)

## Objective

This project will create the necessary data engineering infrastructure to evaluate trends in electricty demand in Colorado.

A data engineering project is more than an ad hoc analysis. It is building a robust system for data collection, ingestion, and transformation in order to create stable, evergreen datasets. Data scientists and analysts will rely on these datasets to be accurate, high quality, and up-to-date, so they can use them for business or research decisions. I also plan to do some data science/analytics work with the data, because why not.

The initial motivation for the project was to fulfil the capstone project requirement for the [2022 Data Engineering Zoomcamp](https://github.com/mharty3/data_engineering_zoomcamp_2022). As such, the goal will be to demonstrate many of the skills I have learned in the zoomcamp lessons.

## High level requirements

* The tool should allow users to access historical electricity demand, EIA demand forecasts, historical weather data, and up-to-date weather forecast data in a Big Query data warehouse
* Initially the scope will be limited to Colorado as an MVP, and more regions can be added in a later phase.
* There should be an interactive dashboard to interact and visualize the data and any models that have been trained on the data

## Data Sources

* Electricity Demand and Generation

  https://www.eia.gov/opendata/

  The United States Energy Information Agency (EIA) provides open access to hundreds of thousands of time series datasets via a REST API. The data is in the public domain, and requires [registraton and an API key](https://www.eia.gov/opendata/register.php).

* Historical Weather Data

  https://registry.opendata.aws/noaa-isd/

  The United States National Oceanic and Atmospheric Administration (NOAA) maintins the Integrated Surface Database (ISD) with global hourly weather station observations from nearly 30,000 stations. The data is available in csv format in open AWS S3 bucket.

* Weather Forecast Data
  * There are several options for this data. I'm currently evaluating them

  * https://registry.opendata.aws/noaa-hrrr-pds/
  * https://registry.opendata.aws/noaa-gfs-bdp-pds/
  * https://registry.opendata.aws/noaa-ndfd/


## Tasks
✅ Find data sources and explore them in notebooks

    ✅ Hourly Energy Demand Data - US EIA

    ✅ Hourly Historical Weather Data - US NOAA

    ✅ Weather Forecast Data - US NOAA

✅ Create GCS Bucket with Terraform to serve as the data lake

✅ Create a Big Query instance with Terraform to serve as the data warehouse

🔲 Ingest raw data into the data lake with Airflow on a proper schedule

    ✅ Hourly Energy Demand Data - US EIA

    🔲 Hourly Historical Weather Data - US NOAA

    🔲 Weather Forecast Data - US NOA


🔲 Transform and Load Data into BQ DW
    ✅ Hourly Energy Demand Data - US EIA

    🔲 Hourly Historical Weather Data - US NOAA

    🔲 Weather Forecast Data - US NO

🔲 Perform additional transformations as needed in the DW with dbt to create analytics layer
    ✅ Hourly Energy Demand Data - US EIA

    🔲 Hourly Historical Weather Data - US NOAA

    🔲 Weather Forecast Data - US NO

✅ Create visualizations in Streamlit

🔲 Train machine learning model to predict energy demand and compare to EIA demand forecast
