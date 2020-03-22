import os
import pandas as pd
import psycopg2
from pandas import ExcelWriter
from datetime import date

try:
    #Dados de conexao com o Banco de dados
    #Atentar para configuracoes de Firewall/seguranca
    conn = psycopg2.connect(dbname="sad", port="5432", user="postgres",
                        password='1234', host="192.168.21.101")

    #Query para selecionar as informacoes relevantes
    query = """SELECT
    sad.unit.formal_abbrd_name_txt OM,
    sad.geo_point.lat_coord AS LATITUDE,
    sad.geo_point.long_coord AS LONGITUDE,
    TO_CHAR(sad.remote_oig.effctv_dttm,'yyyy-mm-dd') AS D1,
    TO_CHAR(sad.checkpoint.effctv_dttm,'yyyy-mm-dd') AS D2,
    TO_CHAR(sad.remote_oig.effctv_dttm,'HH-MM-ss') AS T1,
    TO_CHAR(sad.checkpoint.effctv_dttm,'HH-MM-ss') AS T2,
    UPPER(sad.remote_oig.name_txt) AS OPERACAO,
    sad.oig.descr_txt AS DESCRICAO
    FROM sad.checkpoint,sad.oig, sad.geo_point,sad.unit,sad.remote_oig
    WHERE 
     sad.checkpoint.oig_global_id = sad.oig.global_id
    AND
     sad.checkpoint.entity_id = sad.geo_point.geo_point_id
    AND
     sad.oig.org_id = sad.unit.unit_id
    AND
     sad.oig.global_id = sad.remote_oig.oig_global_id"""

    cursor = conn.cursor()
    cursor.execute(query)

    SQL_Query = pd.read_sql_query(query,conn)
    #Escrita da consulta num arquivo .xlsx
    data_atual = date.today()
    writer = ExcelWriter(r'BD_C2Cmb_Info' + str(data_atual) + '.xlsx')
    SQL_Query.to_excel(writer,'Sheet1')
    writer.save()

    print ("O arquivo foi gerado com sucesso")

except (Exception, psycopg2.Error) as error :
    print ("Erro na conexao com o PostgreSQL ou escrita do arquivo", error)

finally:
    #Fechando a conexao com o Banco de Dados
    if(conn):
        cursor.close()
        conn.close()
        print("Conexao com BD PostgreSQL encerrada")