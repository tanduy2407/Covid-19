import requests
import kaggle
import pandas as pd
import numpy as np
from datetime import date, datetime
from connect_postgres import *
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


WORKING_PATH = '/usr/local/airflow'


# use rapid api to get info for all countries
def get_summary_data():
	params = ['asia', 'africa', 'europe',
			  'northamerica', 'southamerica', 'australia']
	sum_data = []
	for param in params:
		url = f"https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/npm-covid-data/{param}"
		headers = {
			'x-rapidapi-host': "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com",
			'x-rapidapi-key': "282e773d97msh20602f20182196ep1bbea3jsn8b10ac27c324"}
		response = requests.request("GET", url, headers=headers).json()
		for country in response:
			rank = country['rank']
			name = country['Country']
			continent = country['Continent']
			infection_risk = country['Infection_Risk']
			serious_critical = country['Serious_Critical']
			active_cases = country['ActiveCases']
			fatality_rate = country['Case_Fatality_Rate']
			test_percentage = country['Test_Percentage']
			recovery_proporation = country['Recovery_Proporation']
			total_cases = country['TotalCases']
			new_cases = country['NewCases']
			total_deaths = country['TotalDeaths']
			new_deaths = country['NewDeaths']
			total_recovered = country['TotalRecovered']
			total_tests = country['TotalTests']
			population = country['Population']
			run_date = date.today().strftime('%Y-%m-%d')
			info = (rank, name, continent, infection_risk, serious_critical, fatality_rate, active_cases, total_cases,
					new_cases, total_deaths, new_deaths, total_recovered, recovery_proporation, total_tests, test_percentage, population, run_date)
			sum_data.append(info)
	print('Crawl summary data successfully')
	query(keyw='Truncate', table_name='SUMMARY_COVID_DATA')
	query(keyw='Insert', table_name='SUMMARY_COVID_DATA', num_cols=17, data_arr=sum_data)


### using kaggleAPI to get vaccine data
def get_vaccine_data():
	try:
		kaggle.api.authenticate()
		kaggle.api.dataset_download_files(
			'gpreda/covid-world-vaccination-progress', path=f'{WORKING_PATH}/data', unzip=True)
		print('Download files successfully')
		file_path = fr'{WORKING_PATH}/data/country_vaccinations.csv'
		df = pd.read_csv(file_path)
		use_cols = ['country', 'date', 'total_vaccinations', 'people_vaccinated',
					'people_fully_vaccinated', 'daily_vaccinations', 'vaccines']
		final_df = df[use_cols]
		final_df = final_df.replace({np.NAN: None})
		final_df['run_date'] = date.today().strftime('%Y-%m-%d')
	except Exception as err:
		raise err


def get_full_vaccines():
	df_with_vaccine = pd.read_csv(
		rf'{WORKING_PATH}/data/country_vaccinations.csv', usecols=['country', 'vaccines'])
	df_with_vaccine = df_with_vaccine.groupby(by='country').max()

	df_no_vaccine = pd.read_csv(
		'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv')
	df_no_vaccine['vaccines'] = ''
	df_no_vaccine = df_no_vaccine.set_index('location')

	### update 'vaccines' column from df_with_vaccine to df_no_vaccine
	list_country = df_with_vaccine.index
	for country in list_country:
		vaccine = df_with_vaccine.loc[country, 'vaccines']
		df_no_vaccine.loc[country, 'vaccines'] = vaccine
		
	### reformat the vaccine df before update to dB
	df_no_vaccine = df_no_vaccine.reset_index()
	final_df_vaccine = df_no_vaccine[df_no_vaccine['vaccines'] != '']
	final_df_vaccine.rename(columns={'location': 'country'}, inplace=True)
	use_cols = ['country', 'iso_code', 'date', 'total_vaccinations',
				'people_vaccinated', 'people_fully_vaccinated', 'daily_vaccinations',
				'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred',
				'people_fully_vaccinated_per_hundred', 'daily_vaccinations_per_million', 'vaccines']
	final_df_vaccine = final_df_vaccine[use_cols]
	final_df_vaccine = final_df_vaccine.replace({np.NaN: None})

	# update daily vaccine data
	vaccine_data = [tuple(row) for row in final_df_vaccine.values]
	query(keyw='Truncate', table_name='VACCINE_COVID_DATA')
	query(keyw='Insert', table_name='VACCINE_COVID_DATA', num_cols=12, data_arr=vaccine_data)


def main(**kwargs):
	create_schema()
	create_summary_table()
	get_summary_data()
	create_vaccine_table()
	get_vaccine_data()
	get_full_vaccines()


dag = DAG(
		dag_id="update_covid_data",
		schedule_interval="@daily",
		default_args={
			"owner": "airflow",
			"retries": 1,
			"retry_delay": timedelta(minutes=5),
			"start_date": datetime(2021, 1, 1),
		},
		catchup=False,
)
first_function_execute = PythonOperator(
		task_id="update_data",
		python_callable=main,
		provide_context=True, dag=dag
	)

first_function_execute