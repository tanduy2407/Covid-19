import kaggle
import os
import pandas as pd
import numpy as np
from datetime import date
from connect_mysql import query


WORKING_PATH = os.path.dirname(os.path.realpath(__file__))


def get_vaccine_data():
	try:
		kaggle.api.authenticate()
		kaggle.api.dataset_download_files(
			'gpreda/covid-world-vaccination-progress', path=f'{WORKING_PATH}/data', unzip=True)
		print('Download files successfully')
		file_path = fr'{WORKING_PATH}\data\country_vaccinations.csv'
		df = pd.read_csv(file_path)
		use_cols = ['country', 'date', 'total_vaccinations', 'people_vaccinated',
					'people_fully_vaccinated', 'daily_vaccinations', 'vaccines']
		final_df = df[use_cols]
		final_df = final_df.replace({np.NAN: None})
		final_df['run_date'] = date.today().strftime('%Y-%m-%d')
	except Exception as err:
		print(err)


# get_full_vaccine_data()
def get_full_vaccines():
	df_with_vaccine = pd.read_csv(rf'{WORKING_PATH}\data\country_vaccinations.csv', usecols=['country', 'vaccines'])
	df_with_vaccine = df_with_vaccine.groupby(by='country').max()
	df_with_vaccine.to_csv(rf'{WORKING_PATH}\data\country_with_vaccines.csv', index=False)

	df_no_vaccine = pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv')
	df_no_vaccine['vaccines'] = ''
	df_no_vaccine = df_no_vaccine.set_index('location')

	list_country = df_with_vaccine.index
	for country in list_country:
		vaccine = df_with_vaccine.loc[country,'vaccines']
		df_no_vaccine.loc[country, 'vaccines'] = vaccine

	df_no_vaccine = df_no_vaccine.reset_index()
	final_df_vaccine = df_no_vaccine[df_no_vaccine['vaccines'] != '']
	final_df_vaccine.rename(columns = {'location':'country'}, inplace = True)
	final_df_vaccine = final_df_vaccine.drop(['daily_vaccinations_raw', 'total_boosters', 'total_boosters_per_hundred'], axis=1)
	final_df_vaccine = final_df_vaccine.replace({np.NaN:None})
	### update daily vaccine data
	vaccine_data = [tuple(row) for row in final_df_vaccine.values]
	query(keyw='Truncate', table_name='VACCINE_COVID_DATA')
	query(keyw='Insert', table_name='VACCINE_COVID_DATA', num_cols=12, data_arr=vaccine_data)
	# final_df_vaccine.to_csv(rf'{WORKING_PATH}\data\full_vaccinations.csv', index=False)

# get_vaccine_data()
get_full_vaccines()
