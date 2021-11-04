from datetime import date
import pandas as pd
import numpy as np


def get_detail_data():
	url = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
	df = pd.read_csv(url)
	use_cols = ['continent', 'location', 'date', 'new_cases', 'new_cases_smoothed', 'new_deaths', 'new_deaths_smoothed']
	final_df = df[use_cols]
	final_df['date'] = pd.to_datetime(df['date'])
	final_df = final_df.replace({np.NAN: None})
	final_df['update_date'] = date.today().strftime('%Y-%m-%d')
	# print(final_df[final_df['location']=='Vietnam'])
	print(final_df)


get_detail_data()
