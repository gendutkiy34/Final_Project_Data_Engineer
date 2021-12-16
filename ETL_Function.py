# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 12:00:47 2021

@author: gendutkiy
"""

import requests
import json
import sqlalchemy
from sqlalchemy import  create_engine
import mysql.connector
import psycopg2


# fungsi connection to DB
def MysqlConn(host,user,pwd,database,port):
    mydb=mysql.connector.connect(host=host, user=user,password=pwd,database=database,port=port)
    return mydb

def PostCon(host,user,password,db,port):
    conn=psycopg2.connect(database=db,user=user, password=password, host=host, port=port)
    return conn

#mysql connection
mydb=MysqlConn('173.82.100.10','digitalskola','skola123','digitalskola','3311')
sqlcur=mydb.cursor()

#postgress connection
pgdb=PostCon('173.82.100.10','admin','admin.123','postgres','5441')
pgcur=pgdb.cursor()


#Function for Get Raw Data
def GetDataRaw(**kwargs):
  ti = kwargs['ti']
  result = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab').json()
  data_raw=result['data']['content']
  ti.xcom_push('data_raw_covid', data_raw)
  print (f'num of data : {len(data_raw)}')
  

#Function for extract from dict to SQL
def ExtractDataRaw(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='get_raw_covid', key='data_raw_covid')
    #raw_data = json.loads(extract_data)
    #print(type(raw_data))
    
    n=0
    e=0
    for dt in raw_data:
        tex=''
        for k,v in zip(dt.keys(),dt.values()):
            if k=='tanggal' or k=='kode_prov' or k=='nama_prov' or k=='kode_kab' or k=='nama_kab' :
                tex += f"'{v}',"
            else:
                tex += f"{v},"
        tex=tex[:len(tex)-1]
        text=f"({tex})"
        sql = (
            f"INSERT INTO temp_raw_covid (tanggal,kode_prov,nama_prov,kode_kab,nama_kab,suspect,suspect_diisolasi,\
            suspect_discarded,closecontact,closecontact_dikarantina,closecontact_discarded,probable_discarded,\
            probable_diisolasi,probable_meninggal,confirmation,confirmation_diisolasi,confirmation_selesai,\
            confirmation_meninggal,suspect_meninggal_harian,closecontact_meninggal_harian) VALUES ({tex})")
        try :
            sqlcur.execute(sql)
            mydb.commit()
            n += 1
        except:
            e += 1
            pass
    print(f'success insert : {n} , error insert : {e}')


#Function for insert to Dim Province
def InserDimProvince():
    mquery = ("SELECT DISTINCT kode_prov,nama_prov FROM temp_raw_covid ")
    sqlcur.execute(mquery)
    data = sqlcur.fetchall()
    i=0
    e=0
    for dt in data:
        pgsql=f"INSERT INTO province (province_id, province_name) VALUES {dt}"
        try :
            pgcur.execute(pgsql)
            pgdb.commit()
            i += 1
        except :
            e += 1
            pass
    print (f'success insert master province : {i} , failed to insert : {e}')


#Function for insert to Dim District
def InsertDimDistrict():
    mquery = ("SELECT DISTINCT kode_kab,kode_prov,nama_kab FROM temp_raw_covid ")
    sqlcur.execute(mquery)
    data = sqlcur.fetchall()
    i=0
    e=0
    for dt in data:
        pgsql=f"INSERT INTO district (district_id, province_id,district_name) VALUES {dt}"
        try :
            pgcur.execute(pgsql)
            pgdb.commit()
            i += 1
        except :
            e += 1
            pass
    print (f'success insert master district : {i} , failed to insert : {e}')
    
    
#Function for insert to Dim Case Covid Case    
def InsertDimCase(**kwargs):
    id_case=1
    list_case=[]
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='get_raw_covid', key='data_raw_covid')
    
    #get case_id & case_name
    for key in raw_data[0]:
        list_item=[]
        case_name=""
        if key=='tanggal' or key=='kode_prov' or key=='nama_prov' or key=='kode_kab' or key=='nama_kab':
            pass
        else :
            if "_" in key :
                if "suspect_" in key :
                    case_name='suspect'
                elif "closecontact_" in key :
                    case_name='closecontact'
                elif "probable_" in key:
                    case_name="probable"
                elif "confirmation_" in key:
                    case_name="confirmation"
                list_item.append(str(id_case))
                list_item.append(case_name)
                list_item.append(key.lower())
                list_case.append(list_item)
                id_case += 1
                
    # push to xcom case id covid
    ti.xcom_push('case covid', list_case)
   
    #insert to table
    i=0
    e=0
    for dt in list_case:
        msql=f"INSERT INTO case_covid (case_id, status_name,status_detail) VALUES ('{dt[0]}','{dt[1]}','{dt[2]}')"
        pgsql=f"INSERT INTO case_covid (case_id, status_name,status_detail) VALUES ('{dt[0]}','{dt[1]}','{dt[2]}')"
        try :
            sqlcur.execute(msql)
            mydb.commit()
            pgcur.execute(pgsql)
            pgdb.commit()
            i += 1
        except:
            e += 1
            pass
    print (f'success insert master case : {i} , failed to insert : {e}')
    
def InsertMyCase(**kwargs):
    ti = kwargs['ti']
    case_data = ti.xcom_pull(task_ids='import_data_master_case', key='case covid')
       
    #insert to table
    i=0
    e=0
    for dt in case_data:
        msql=f"INSERT INTO case_covid (case_id, status_name,status_detail) VALUES ('{dt[0]}','{dt[1]}','{dt[2]}')"
        print (msql)
        try :
            sqlcur.execute(msql)
            mydb.commit()
            i += 1
        except:
            e += 1
            pass
    print (f'success insert master case : {i} , failed to insert : {e}')
           
                
    
