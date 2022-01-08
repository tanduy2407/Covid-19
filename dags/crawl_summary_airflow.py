import requests
from datetime import date, datetime
from connect_postgres import *
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from crawl_vaccine_airflow import get_full_vaccines, get_vaccine_data

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