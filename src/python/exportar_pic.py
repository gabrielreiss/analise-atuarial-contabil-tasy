import cx_Oracle #EXECUTAR COMO ADMINISTRADOR
import pandas as pd
import time
import os
import numpy as np

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

    df['FIC12'] = ((df['EIL'].rolling(12).sum() + df['DC'].rolling(12).sum() + df['DA'].rolling(12).sum() - df['MULTASADM'].rolling(12).sum())/ df['CONTRAPRESTACOES_LIQUIDAS'].rolling(12).sum()) - 1 
    df['FIC24'] = ((df['EIL'].rolling(24).sum() + df['DC'].rolling(24).sum() + df['DA'].rolling(24).sum() - df['MULTASADM'].rolling(24).sum())/ df['CONTRAPRESTACOES_LIQUIDAS'].rolling(24).sum()) - 1 
    df['FIC'] = np.minimum(df['FIC12'], df['FIC24'])
    df['PIC_Calculada'] = df['CONTRAPRESTACOES_MEDICO_HOSP'].rolling(12).sum() * df['FIC'] 
    df = df.dropna()

    output_file = os.path.join(OUTPUT_DIR, f'{nome}.csv')
    df.to_csv(output_file, mode='w', index=False, encoding='latin-1', sep=';', decimal=',', date_format="%Y-%m-%d", header=True)

    end_time = time.time()
    end_time_formatted = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
    execution_time = int(end_time - start_time)
    print(f"Exportação {nome} encerrada em: {end_time_formatted} - Tempo de execução: {execution_time} segundos")


#### Salvando arquivo
exportar_csv('pic')