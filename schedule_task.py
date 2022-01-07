from crawl_summary import get_summary_data
from dags.crawl_vaccine_airflow import get_full_vaccines, get_vaccine_data
from connect_mysql import query

def main():
	summary_data = get_summary_data()
	query(keyw='Truncate', table_name='SUMMARY_COVID_DATA')
	query(keyw='Insert', table_name='SUMMARY_COVID_DATA', num_cols=17, data_arr=summary_data)
	get_vaccine_data()
	get_full_vaccines()

main()