# -*- coding: utf-8 -*-
"""
Created on Wed Dec 15 10:31:26 2021

@author: gendutkiy
"""

import requests
import json
import sqlalchemy
from sqlalchemy import  create_engine
import mysql.connector
import psycopg2
import datetime


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

#Insert to Postgress staging CloseContact
def InsertStagingCloseContact():
    mquery = ("SELECT tanggal,kode_prov,kode_kab,closecontact_discarded,closecontact_meninggal,closecontact_dikarantina FROM temp_raw_covid ")
    sqlcur.execute(mquery)
    data = sqlcur.fetchall()
    i=0
    e=0
    for dt in data:
        #x=dt[0].strftime("%Y-%m-%d")
        data_pg=f"'{dt[0]}',{dt[1]},{dt[2]},{dt[3]},{dt[4]},{dt[5]}"
        pgsql=f"INSERT INTO staging_closecontact (tanggal,kode_prov,kode_kab,closecontact_discarded,closecontact_meninggal,closecontact_dikarantina) VALUES ({data_pg})"
        try:
            pgcur.execute(pgsql)
            pgdb.commit()
            i +=1
        except:
            e +=1
            pass
    print (f'Insert to staging_closecontact, success : {i} , failed : {e}')

#Insert to Postgress staging Suspect
def InsertStagingSuspect():
    mquery = ("SELECT tanggal,kode_prov,kode_kab,suspect_diisolasi,suspect_discarded,suspect_meninggal FROM temp_raw_covid ")
    sqlcur.execute(mquery)
    data = sqlcur.fetchall()
    i=0
    e=0
    for dt in data:
        #x=dt[0].strftime("%Y-%m-%d")
        data_pg=f"'{dt[0]}',{dt[1]},{dt[2]},{dt[3]},{dt[4]},{dt[5]}"
        pgsql=f"INSERT INTO staging_suspect (tanggal,kode_prov,kode_kab,suspect_diisolasi,suspect_discarded,suspect_meninggal) VALUES ({data_pg})"
        try:
            pgcur.execute(pgsql)
            pgdb.commit()
            i +=1
        except:
            e +=1
            pass
    print (f'Insert to staging_suspect, success : {i} , failed : {e}')

#Insert to Postgress staging Probable
def InsertStagingProbable():
    mquery = ("SELECT tanggal,kode_prov,kode_kab,probable_diisolasi,probable_discarded,probable_meninggal FROM temp_raw_covid ")
    sqlcur.execute(mquery)
    data = sqlcur.fetchall()
    i=0
    e=0
    for dt in data:
        #x=dt[0].strftime("%Y-%m-%d")
        data_pg=f"'{dt[0]}',{dt[1]},{dt[2]},{dt[3]},{dt[4]},{dt[5]}"
        pgsql=f"INSERT INTO staging_probable (tanggal,kode_prov,kode_kab,probable_discarded,probable_diisolasi,probable_meninggal) VALUES ({data_pg})"
        try:
            pgcur.execute(pgsql)
            pgdb.commit()
            i +=1
        except:
            e +=1
            pass
    print (f'Insert to staging_probable, success : {i} , failed : {e}')
    
    
def InsertStagingconfirmation():
    mquery = ("SELECT tanggal,kode_prov,kode_kab,confirmation_meninggal,confirmation_sembuh FROM temp_raw_covid ")
    sqlcur.execute(mquery)
    data = sqlcur.fetchall()
    i=0
    e=0
    for dt in data:
        #x=dt[0].strftime("%Y-%m-%d")
        data_pg=f"'{dt[0]}',{dt[1]},{dt[2]},{dt[3]},{dt[4]}"
        pgsql=f"INSERT INTO staging_confirmation (tanggal,kode_prov,kode_kab,confirmation_meninggal,confirmation_sembuh) VALUES ({data_pg})"
        try:
            pgcur.execute(pgsql)
            pgdb.commit()
            i +=1
        except:
            e +=1
            pass
    print (f'Insert to staging_confirmation, success : {i} , failed : {e}')