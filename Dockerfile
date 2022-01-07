FROM puckel/docker-airflow:1.10.9
# FROM apache/airflow:2.0.0-python3.7
COPY dags /usr/local/airflow/dags
COPY .kaggle /usr/local/airflow/.kaggle
RUN pip install requests
RUN pip install pandas
RUN pip install numpy
RUN pip install kaggle
RUN pip install psycopg2-binary