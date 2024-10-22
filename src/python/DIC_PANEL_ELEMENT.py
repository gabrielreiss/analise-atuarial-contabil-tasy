import cx_Oracle #EXECUTAR COMO ADMINISTRADOR
import pandas as pd
import os

BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
ORACLE_DIR = os.path.join(os.path.dirname(BASE_DIR), 'instantclient-basic-windows.x64-23.4.0.24.05', 'instantclient_23_4')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
SQL_DIR = os.path.join(BASE_DIR, 'src', 'sql')
SQL_TASY_DIR = os.path.join(BASE_DIR, 'src', 'sql_DIC_PANEL_ELEMENT')

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

df = pd.read_sql_query(ler_query('0select_DIC_PANEL_ELEMENT'), connection, parse_dates="%Y-%m-%d")

for index, row in df.iterrows():
    if pd.isna(row['DS_TITLE_ATTRIBUTE']):
        with open(os.path.join(SQL_TASY_DIR, f'{row['NR_SEQUENCIA']}.sql'), "w", encoding="utf-8") as f:
            f.write(f'--{row['NR_SEQUENCIA']}\n--{row['DS_TITLE_ATTRIBUTE']}\n{row['DS_SQL']}')
    else:
        nome = row['DS_TITLE_ATTRIBUTE'].replace('/','-').replace('\\','-').replace("\\\\", "").replace("\\\\\\", "").replace(',','').replace(';','').replace('\r', 'r').replace('\n', 'n').replace('\t', 't').replace('|', '')
        if not os.path.exists(os.path.join(SQL_TASY_DIR, f'{nome}')):
            #print(f'Salvando: {row['NM_OBJETO']}')
            try:
                with open(os.path.join(SQL_TASY_DIR, f'{row['NR_SEQUENCIA']}-{row['DS_TITLE_ATTRIBUTE']}.sql'), "w", encoding="utf-8") as f:
                    f.write(f'--{row['NR_SEQUENCIA']}\n--{row['DS_TITLE_ATTRIBUTE']}\n{row['DS_SQL']}')
            except Exception as e:
                print(e)
