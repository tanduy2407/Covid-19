FROM puckel/docker-airflow:1.10.9
# FROM apache/airflow:2.0.0-python3.7
COPY dags /usr/local/airflow/dags
COPY .kaggle /usr/local/airflow/.kaggle
RUN pip install requests==2.26.0
RUN pip install pandas==1.3.1
RUN pip install numpy==1.19.5
RUN pip install kaggle==1.5.12
RUN pip install psycopg2-binary==2.9.3