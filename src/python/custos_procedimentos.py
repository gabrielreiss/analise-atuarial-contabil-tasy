#%%
import cx_Oracle #EXECUTAR COMO ADMINISTRADOR
import pandas as pd
import time
import os
#import math
#import datetime
#import numpy as np
#from workdays import networkdays as nt
#import workdays as wd

BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
ORACLE_DIR = os.path.join(os.path.dirname(BASE_DIR), 'instantclient-basic-windows.x64-23.4.0.24.05', 'instantclient_23_4')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
SQL_DIR = os.path.join(BASE_DIR, 'src', 'sql')

try:
    cx_Oracle.init_oracle_client(lib_dir=ORACLE_DIR)
except Exception as e:
    print(e)

from dotenv import load_dotenv
load_dotenv()

user_name = os.getenv('USER')
pass_key = os.getenv('PASSWORD')
porta = os.getenv('PORTA')
service_name = os.getenv('SERVICE_NAME')
banco_name = os.getenv('BANCO')

dsn = cx_Oracle.makedsn(banco_name, porta, service_name= service_name) 
user = user_name #PODE USAR O PRÓPRIO USUÁRIO E SENHA
password = pass_key

try: 
    connection = cx_Oracle.connect(user, password, dsn)
    print("Conexão estabelecida...")
except Exception as e:
    print(e)

def ler_query(nome:str):
    with open(os.path.join(SQL_DIR, f'{nome}.sql'), "r", encoding="utf-8") as f:
        sql = f.read()
    return sql

def exportar_csv(nome:str):
    start_time = time.time()
    start_time_formatted = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
    print(f"Exportação {nome} iniciada em: {start_time_formatted}")

    sql = ler_query(f'{nome}')
    df = pd.read_sql_query(sql, connection, parse_dates="%Y-%m-%d")
    
    end_time = time.time()
    end_time_formatted = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
    execution_time = int(end_time - start_time)
    print(f"Exportação {nome} encerrada em: {end_time_formatted} - Tempo de execução: {execution_time} segundos")
    return df

#%%
#### Salvando arquivo
df_procedimentos_verte = exportar_csv('verte_sede_procedimentos')
df_alocacao_individual = exportar_csv('verte_sede_alocacao_reembolso')
df_alocacao_agrupado   = exportar_csv('verte_sede_alocacao_reembolso_agrupado')

columns = ['COMPETENCIA', 'CD_CONTA_DEBITO', 'TOTAL']
total = df_alocacao_agrupado.query('CD_HISTORICO == 1083')[columns]

mapa = {
        'MODALIDADE': [
                    'IF_medico', 
                    'IF_Odonto', 
                    'CA_Pos', 
                    'CA_Pos_Odonto', 
                    'CE_Pos', 
                    'CE_Pos_Odonto', 
                    'CA_Pre', 
                    'CA_Pre_Odonto',
                    'Titulo_Associativo',
                    'Benef_Compl',
                    '4.4.2.1.1.9.0.1.9.9.03', 
                    'Ortodontia'],
        'CD_CONTA_DEBITO': [
                    '44751', #IF_medico
                    '44754', #IF_Odonto
                    '44752', #CA_Pos
                    '44755', #CA_Pos_Odonto
                    '44753', #CE_Pos
                    '44756', #CE_Pos_Odonto
                    '40019', #CA_Pre
                    '40072', #CA_Pre_Odonto
                    '44064', # Titulo_Associativo
                    '44065', #Benef_Compl
                    '44076', #4.4.2.1.1.9.0.1.9.9.03
                    '46151'] #Ortodontia
}
mapeamento = dict(zip(mapa['CD_CONTA_DEBITO'], mapa['MODALIDADE']))
total['MODALIDADE'] = total['CD_CONTA_DEBITO'].map(mapeamento)

grouped_df2 = df_procedimentos_verte.groupby(['COMPETENCIA', 'MODALIDADE', 'CD_PROCEDIMENTO'])['QNT'].sum().reset_index()

grouped_df2 = grouped_df2.merge(total, left_on=['COMPETENCIA', 'MODALIDADE', 'CD_PROCEDIMENTO'],
                       right_on=['COMPETENCIA', 'MODALIDADE', 'CD_PROCEDIMENTO'], how='left')

grouped_df2['VALOR_PROPORCIONAL'] = (grouped_df2['TOTAL'] / grouped_df2['QNT']) * df_procedimentos_verte['QNT']
grouped_df2['VALOR_TOTAL'] = grouped_df2['VALOR_PROPORCIONAL'] + grouped_df2['TOTAL']
grouped_df2['NOVA_MEDIA_VERTE'] = grouped_df2['VALOR_TOTAL']/df_procedimentos_verte['QNT']

demais_prestadores   = exportar_csv('custo_procedimentos_prestadores')

grouped_df2['MEDIA_VERTE'] = grouped_df2['TOTAL'] / df_procedimentos_verte['QNT']
columns = ['COMPETENCIA','MODALIDADE', 'CD_PROCEDIMENTO', 'TOTAL', 'QNT', 'VALOR_PROPORCIONAL', 'VALOR_TOTAL', 'MEDIA_VERTE', 'NOVA_MEDIA_VERTE']
grouped_df2 = grouped_df2[columns]
grouped_df2.columns = ['COMPETENCIA','MODALIDADE', 'CD_PROCEDIMENTO', 'TOTAL_VERTE', 'QNT_VERTE', 'VALOR_PROPORCIONAL', 'VALOR_TOTAL', 'MEDIA_VERTE', 'NOVA_MEDIA_VERTE']

grouped_df2 = grouped_df2.merge(demais_prestadores, how='outer', left_on=['COMPETENCIA', 'MODALIDADE', 'CD_PROCEDIMENTO'], right_on=['COMPETENCIA', 'MODALIDADE', 'CD_PROCEDIMENTO'])
nome = 'custos_verte_procedimento'
output_file = os.path.join(OUTPUT_DIR, f'{nome}.csv')
grouped_df2.to_csv(output_file, mode='w', index=False, encoding='latin-1', sep=';', decimal=',', date_format="%Y-%m-%d", header=True)

#%%
