FROM puckel/docker-airflow:1.10.9
COPY dags /usr/local/airflow/dags
RUN pip install requests
RUN pip install pandas
RUN pip install mysql

