import MySQLdb
import pandas as pd


def connectDB():
	try:
		connection = MySQLdb.connect(
			host="127.0.0.1",
			port=3306,
			user="root",
			passwd="tanduy2407",
			charset='utf8mb4',
			use_unicode=True)
		cursor = connection.cursor()
		print('Connect Database successfully')
	except Exception as err:
		print(err)
	return connection, cursor


conn, cursor = connectDB()


def query(keyw, table_name, num_cols=None, data_arr=None):
	if keyw == 'Read':
		sql = f'SELECT * FROM covid_data.{table_name}'
		df = pd.read_sql_query(sql, conn)
		return df

	if keyw == 'Insert':
		bind = ', '.join(["%s" for _ in range(num_cols)])
		sql = f"INSERT INTO covid_data.{table_name} VALUES ({bind})"
		try:
			cursor.executemany(sql, data_arr)
			conn.commit()
			print(f'Insert data {len(data_arr)} rows to {table_name}')
		except Exception as err:
			print(err)

	if keyw == 'Truncate':
		sql = f'TRUNCATE TABLE covid_data.{table_name}'
		try:
			cursor.execute(sql)
			print(f'Truncate table {table_name}')
		except Exception as err:
			print(err)


def create_summary_table():
	try:
		sql = """CREATE TABLE covid_data.SUMMARY_COVID_DATA
					(RANKING INT,
					NAME VARCHAR(255),
					CONTINENT VARCHAR(255),
					INFECTION_RISK FLOAT,
					SERIOUS_CRITICAL INT,
					FATALITY_RATE FLOAT,
					ACTIVE_CASES INT,
					TOTAL_CASES INT,
					NEW_CASES INT,
					TOTAL_DEATHS INT,
					NEW_DEATHS INT,
					TOTAL_RECOVERED INT,
					RECOVERY_PROPORATION FLOAT,
					TOTAL_TESTS INT,
					TEST_PERCENTAGE FLOAT,
					POPULATION INT, 
					RUN_DATE DATE)"""
		cursor.execute(sql)
		print('Create table success')
	except Exception as err:
		print(err)

# create_summary_table()
def create_vaccine_table():
	try:
		sql = """CREATE TABLE covid_data.VACCINE_COVID_DATA
					(COUNTRY VARCHAR(100),
					ISO_CODE VARCHAR(10),
					DATE DATE,
					TOTAL_VACCINATIONS BIGINT,
					PEOPLE_VACCINATED BIGINT,
					PEOPLE_FULLY_VACCINATED BIGINT,
					DAILY_VACCINATIONS INT,
					TOTAL_VACCINATIONS_PER_HUNDRED FLOAT,
					PEOPLE_VACCINATED_PER_HUNDRED FLOAT,
					PEOPLE_FULLY_VACCINATED_PER_HUNDRED FLOAT,
					DAILY_VACCINATIONS_PER_MILLION FLOAT,
					VACCINES VARCHAR(300))"""
		cursor.execute(sql)
		print('Create table success')
	except Exception as err:
		print(err)

# create_vaccine_table()

def create_detail_table():
	try:
		sql = """CREATE TABLE covid_data.DETAIL_COVID_DATA(
				COUNTRY VARCHAR(100),
				DATE DATE,
				TOTAL_CONFIRMED BIGINT,
				TOTAL_DEATHS BIGINT,
				MORTALITY_RATE FLOAT)"""
		cursor.execute(sql)
		print('Create table success')
	except Exception as err:
		print(err)

create_detail_table()