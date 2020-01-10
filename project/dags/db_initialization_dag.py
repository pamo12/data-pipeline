from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator

import sql_statements

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False
}

dag = DAG('db_initialization_dag',
          default_args=default_args,
          description='Create Tables within Redshift',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_staging_events = PostgresOperator(
    task_id="drop_staging_events",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_STAGE_EVENTS
)

create_staging_events = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGE_EVENTS
)

drop_staging_songs = PostgresOperator(
    task_id="drop_staging_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_STAGE_SONGS
)

create_staging_songs = PostgresOperator(
    task_id="create_staging_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGE_SONGS
)

drop_artists = PostgresOperator(
    task_id="drop_artists",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_ARTISTS
)

create_artists = PostgresOperator(
    task_id="create_artists",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_ARTISTS
)

drop_songs = PostgresOperator(
    task_id="drop_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_SONGS
)

create_songs = PostgresOperator(
    task_id="create_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGS
)

drop_users = PostgresOperator(
    task_id="drop_users",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_USERS
)

create_users = PostgresOperator(
    task_id="create_users",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_USERS
)

drop_time = PostgresOperator(
    task_id="drop_time",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_TIME
)

create_time = PostgresOperator(
    task_id="create_time",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_TIME
)

drop_songplays = PostgresOperator(
    task_id="drop_songplays",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_SONGPLAYS
)

create_songplays = PostgresOperator(
    task_id="create_songplays",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGPLAYS
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_staging_events
drop_staging_events >> create_staging_events
create_staging_events >> end_operator

start_operator >> drop_staging_songs
drop_staging_songs >> create_staging_songs
create_staging_songs >> end_operator

start_operator >> drop_artists
drop_artists >> create_artists
create_artists >> end_operator

start_operator >> drop_songs
drop_songs >> create_songs
create_songs >> end_operator

start_operator >> drop_users
drop_users >> create_users
create_users >> end_operator

start_operator >> drop_time
drop_time >> create_time
create_time >> end_operator

start_operator >> drop_songplays
drop_songplays >> create_songplays
create_songplays >> end_operator