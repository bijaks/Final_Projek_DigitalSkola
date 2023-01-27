from datetime import datetime
import logging

from airflow import DAG
from airflow.models import Variable,Connection
from airflow.operators.python import PythonOperator

from modules.connection import Connectionn
from modules.data_load import Dataload
from modules.tranform import Tranform

def func_get_data_from_api(**kwargs):
    #get data
    extract = Dataload(Variable.get('url_covid'))
    data = extract.get_data()
    print(data.info())
    
    #create connector
    get_conn = Connection.get_connection_from_secrets("Mysql")
    conn = Connectionn()
    engine_mysql = conn.conn_mysql(
        host=get_conn.host,
        user=get_conn.login,
        password=get_conn.password,
        db=get_conn.schema,
        port=get_conn.port
    )
    
    #drop table if exist
    try:
        p = 'DROP TABLE IF EXISTS covid_jabar'
        engine_mysql.execute(p)
    except Exception as e:
        logging.error(e)
    try:
        data.to_sql(con=engine_mysql,name='covid_jabar',index=False)
        logging.info("DATA BERHASIL DIMASUKKAN KE MYSQL")
    except Exception as e:
        logging.error(e)
    
def func_show_data_mysql(**kwargs):
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    conn = Connectionn()
    engine_mysql = conn.conn_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = conn.conn_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
   
    #insert data 
    tranform = Tranform(engine_mysql,engine_postgres)
    tranform = tranform.get_data_from_mysql()
def func_generate_dim(**kwargs):
    #create connector
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    conn = Connectionn()
    engine_mysql = conn.conn_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = conn.conn_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
   
    #insert data 
    tranform = Tranform(engine_mysql,engine_postgres)
    tranform.create_dimension_case()
    tranform.create_dimension_district()
    tranform.create_dimension_province()
    print("dah disini artinyo masuk")
    
def func_province_daily(**kwargs):
    #create connector
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    conn = Connectionn()
    engine_mysql = conn.conn_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = conn.conn_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
    
    #insert data 
    tranform = Tranform(engine_mysql,engine_postgres)
    tranform.create_province_daily()

    
def func_district_daily(**kwargs):
    #create connector
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    conn = Connectionn()
    engine_mysql = conn.conn_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = conn.conn_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
    
    #insert data 
    tranform = Tranform(engine_mysql,engine_postgres)
    tranform.create_district_daily()

with DAG(
    dag_id='d_1_final_porject',
    start_date = datetime(2023,1,27),
    schedule_interval = '0 0 * * *',
    catchup = False
) as dag:
    
    opr_get_data_from_api = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable = func_get_data_from_api
    )
    
    opr_generate_dim = PythonOperator(
        task_id = 'generate_dim',
        python_callable = func_generate_dim
    )
    
    opr_insert_province_daily = PythonOperator(
        task_id = 'insert_province_daily',
        python_callable = func_province_daily
    )
    
    opr_insert_district_daily = PythonOperator(
        task_id = 'insert_district_daily',
        python_callable = func_district_daily
    )
    
opr_get_data_from_api >> opr_generate_dim
opr_generate_dim >> opr_insert_province_daily
opr_generate_dim >> opr_insert_district_daily 
    
    