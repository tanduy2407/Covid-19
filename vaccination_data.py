import kaggle
import os
import pandas as pd
import numpy as np
from datetime import date


WORKING_PATH = os.path.dirname(os.path.realpath(__file__))


def get_vaccine_data():
	try:
		kaggle.api.authenticate()
		kaggle.api.dataset_download_files(
			'gpreda/covid-world-vaccination-progress', path='D:/PycharmProjects/Python/Covid-19/data', unzip=True)
		print('Download files successfully')
		file_path = fr'{WORKING_PATH}\data\country_vaccinations.csv'
		df = pd.read_csv(file_path)
		use_cols = ['country', 'date', 'total_vaccinations', 'people_vaccinated',
					'people_fully_vaccinated', 'daily_vaccinations', 'vaccines']
		final_df = df[use_cols]
		final_df['date'] = pd.to_datetime(df['date'])
		final_df = final_df.replace({np.NAN: None})
		final_df['run_date'] = date.today().strftime('%Y-%m-%d')
		# print(final_df[final_df['country']=='Zimbabwe'])
		print(len(final_df))
	except Exception as err:
		print(err)


get_vaccine_data()

