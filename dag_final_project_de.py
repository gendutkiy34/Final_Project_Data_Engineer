# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 12:02:56 2021

@author: gendutkiy
"""


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from script.ETL_Function import GetDataRaw,ExtractDataRaw,InserDimProvince
from script.ETL_Function import InsertDimDistrict,InsertDimCase,InsertMyCase
from script.InsertDataStaging import InsertStagingCloseContact,InsertStagingSuspect,InsertStagingProbable,InsertStagingconfirmation
import requests 
import json



default_args = {
    'owner': 'hariono',
    'depends_on_past': False,
    'email_on_failuer': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('ETL_Data_Covid_Jabar',
    start_date=datetime(2021,12,12),
    default_args={'mysql_conn_id':'mysql_rackner'}
) as dag:
    starting = DummyOperator(
        task_id='start'
    )

    Get_Raw_Data = PythonOperator(
        task_id='get_raw_covid',
        python_callable=GetDataRaw,
		provide_context=True,
    )
   
    ImportMasterProvince = PythonOperator(
        task_id='import_data_master_province',
        python_callable=InserDimProvince,
        provide_context=True,
    )
    
    ImportMasterCase = PythonOperator(
        task_id='import_data_master_case',
        python_callable=InsertDimCase,
        provide_context=True,
    )
    
    ImportMasterDistrict = PythonOperator(
        task_id='import_data_master_district',
        python_callable=InsertDimDistrict,
        provide_context=True,
    )
    
    ImportStagingClose = PythonOperator(
        task_id='insert_to_staging_closecontact',
        python_callable=InsertStagingCloseContact,
        provide_context=True,
    )
    
    ImportStagingSuspect = PythonOperator(
        task_id='insert_to_staging_suspect',
        python_callable=InsertStagingSuspect,
        provide_context=True,
    )
    
    ImportStagingProblable = PythonOperator(
        task_id='insert_to_staging_problable',
        python_callable=InsertStagingProbable,
        provide_context=True,
    )
    
    ImportStagingConfirmation = PythonOperator(
        task_id='insert_to_staging_confirmation',
        python_callable=InsertStagingconfirmation,
        provide_context=True,
    )
    
    InsertFactProvYear = PostgresOperator(
        task_id="insert_to_fact_province_yearly",
        postgres_conn_id='progress_rackner',
        sql='sql/yearly_province.sql'
    )
    
    InsertFactDisYear = PostgresOperator(
        task_id="insert_to_fact_district_yearly",
        postgres_conn_id='progress_rackner',
        sql='sql/yearly_district.sql'
    )
    
    InsertFactProvMon = PostgresOperator(
        task_id="insert_to_fact_Province_Yearly",
        postgres_conn_id='progress_rackner',
        sql='sql/monthly_province.sql'
    )
    
    InsertFactDisMon = PostgresOperator(
        task_id="insert_to_fact_district_monthly",
        postgres_conn_id='progress_rackner',
        sql='sql/monthly_district.sql'
    )
    
    InsertFactProvDay = PostgresOperator(
        task_id="insert_to_fact_Province_Daily",
        postgres_conn_id='progress_rackner',
        sql='sql/daily_province.sql'
    )
    
    TruncTable = PostgresOperator(
        task_id="Clear_Table",
        postgres_conn_id='progress_rackner',
        sql='sql/truncate_table_dim.sql'
    )

    starting >> Get_Raw_Data  >> TruncTable >> [ImportStagingClose,ImportStagingSuspect,ImportStagingProblable,ImportStagingConfirmation] 
    [ImportStagingClose,ImportStagingSuspect,ImportStagingProblable,ImportStagingConfirmation] >> ImportMasterProvince >> [ImportMasterDistrict,ImportMasterCase ]
    [ImportMasterDistrict,ImportMasterCase ] >> InsertFactProvDay >> [InsertFactProvYear,InsertFactProvMon,InsertFactDisYear,InsertFactDisMon]
    #ImportMasterCase >> InsertFactProvYear >> InsertFactProvMon >> InsertFactProvDay
    #ImportMasterDistrict >> InsertFactDisYear >> InsertFactDisMon
    #starting >> Get_Raw_Data >> Extract_data >> [ImportMasterProvince,ImportMasterCase ] >>  ImportMasterDistrict  
    #Extract_data >> [ImportStagingClose,ImportStagingSuspect,ImportStagingProblable,ImportStagingConfirmation]