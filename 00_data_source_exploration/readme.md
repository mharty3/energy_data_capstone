# Data Source Exploration Notebooks

These notebooks explore and document the data sources used in the project. Code from these notebooks was modified in the airflow DAGs to regularly ingest the data from the sources into the data lake on Google Cloud. 

To run the code, you will need to create a conda environment containing the dependencies. You can either create one from the `environment.yml` file with the command: 

```bash
conda env create -f environment.yml
```

or create the environment yourself with the following commands:

```bash
conda create -n energy jupyter requests cartopy xarray
conda activate energy
pip install python-dotenv pygrib
```

then activate the environment: `conda activate energy`, and run `jupyter notebook`.

## API Keys
To access the data from the EIA API and the Open Weather Map API, you will need to register for your own API key [with the EIA](https://www.eia.gov/opendata/register.php) and [OWM](https://home.openweathermap.org/users/sign_up) for free. When they email you the key, place it in a file called `.env` located in this directory and be sure to add that file to your .gitignore so you don't commit it to your repo. The file should look like this:

```
EIA_KEY=your_eia_api_key
OWM_KEY=your_owm_api_key
```
