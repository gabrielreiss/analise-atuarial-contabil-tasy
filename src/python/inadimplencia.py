#%%
import cx_Oracle #EXECUTAR COMO ADMINISTRADOR
import pandas as pd
import time
import os

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

#%%
def ler_query(nome:str):
    with open(os.path.join(SQL_DIR, f'{nome}.sql'), "r", encoding="utf-8") as f:
        sql = f.read()
    return sql

#def exportar_csv(nome:str):
nome = 'inadimplencia'
start_time = time.time()
start_time_formatted = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
print(f"Exportação {nome} iniciada em: {start_time_formatted}")

sql = ler_query(f'{nome}')
df = pd.read_sql_query(sql, connection, parse_dates="%Y-%m-%d")

#%%
colunas = [
    'CRED_IF_MEDICO',
    'CRED_ADESAOPOS_MEDICO',
    'CRED_EMP_MEDICO',
    'CRED_ADESAOPRE_MEDICO',
    'CRED_TITULO_ASSOC',
    'CRED_IPE_COMPL_MEDICO',
    'PERDA_IF_MEDICO_PERDAS',
    'PERDA_ADESAOPOS_MEDICO_PERDAS',
    'PERDA_EMP_MEDICO_PERDAS',
    'PERDA_ADESAOPRE_MEDICO_PERDAS',
    'PERDA_TITULO_ASSOC_PERDAS',
    'PERDA_IPE_COMPL_MEDICO',
    'CRED_IF_ODONTO',
    'CRED_ADESAOPOS_ODONTO',
    'CRED_EMP_ODONTO',
    'CRED_ADESAOPRE_ODONTO',
    'CRED_IPE_COMPL_ODONTO',
    'PERDA_IF_ODONTO_PERDAS',
    'PERDA_ADESAOPOS_ODONTO_PERDAS',
    'PERDA_EMP_ODONTO_PERDAS',
    'PERDA_ADESAOPRE_ODONTO_PERDAS',
    'PERDA_IPE_COMPL_ODONTO',
    'CRED_IF_MEDICO_VAR',
    'CRED_ADESAOPOS_MEDICO_VAR',
    'CRED_EMP_MEDICO_VAR',
    'CRED_ADESAOPRE_MEDICO_VAR',
    'CRED_TITULO_ASSOC_VAR',
    'CRED_IPE_COMPL_MEDICO_VAR',
    'PERDA_IF_MEDICO_PERDAS_VAR',
    'PERDA_ADESAOPOS_MEDICO_PERDAS_VAR',
    'PERDA_EMP_MEDICO_PERDAS_VAR',
    'PERDA_ADESAOPRE_MEDICO_PERDAS_VAR',
    'PERDA_TITULO_ASSOC_PERDAS_VAR',
    'PERDA_IPE_COMPL_MEDICO_VAR',
    'CRED_IF_ODONTO_VAR',
    'CRED_ADESAOPOS_ODONTO_VAR',
    'CRED_EMP_ODONTO_VAR',
    'CRED_ADESAOPRE_ODONTO_VAR',
    'CRED_IPE_COMPL_ODONTO_VAR',
    'PERDA_IF_ODONTO_PERDAS_VAR',
    'PERDA_ADESAOPOS_ODONTO_PERDAS_VAR',
    'PERDA_EMP_ODONTO_PERDAS_VAR',
    'PERDA_ADESAOPRE_ODONTO_PERDAS_VAR',
    'PERDA_IPE_COMPL_ODONTO_VAR',
    'CRED_TOTAL',
    'PERDA_TOTAL',
    'CRED_TOTAL_VAR',
    'PERDA_TOTAL_VAR',
    'RECEITA_IF_MEDICO',
    'RECEITA_ADESAOPOS_MEDICO',
    'RECEITA_EMP_MEDICO',
    'RECEITA_ADESAOPRE_MEDICO',
    'RECEITA_TITULO_ASSOC',
    'RECEITA_IPE_COMPL',
    'RECEITA_IF_ODONTO',
    'RECEITA_ADESAOPOS_ODONTO',
    'RECEITA_EMP_ODONTO',
    'RECEITA_ADESAOPRE_ODONTO',
    'RECEITA_IPE_COMPL_ODONTO',
    'RECEITA_TOTAL',
    'RECUP_IF_MEDICO',
    'RECUP_ADESAOPOS_MEDICO',
    'RECUP_EMP_MEDICO',
    'RECUP_ADESAOPRE_MEDICO',
    'RECUP_TITULO_ASSOC',
    'RECUP_IPE_COMPL_MEDICO',
    'RECUP_IF_ODONTO',
    'RECUP_ADESAOPOS_ODONTO',
    'RECUP_EMP_ODONTO',
    'RECUP_ADESAOPRE_ODONTO',
    'RECUP_IPE_COMPL_ODONTO',
    'RECUP_IF_MEDICO_VAR',
    'RECUP_ADESAOPOS_MEDICO_VAR',
    'RECUP_EMP_MEDICO_VAR',
    'RECUP_ADESAOPRE_MEDICO_VAR',
    'RECUP_TITULO_ASSOC_VAR',
    'RECUP_IPE_COMPL_MEDICO_VAR',
    'RECUP_IF_ODONTO_VAR',
    'RECUP_ADESAOPOS_ODONTO_VAR',
    'RECUP_EMP_ODONTO_VAR',
    'RECUP_ADESAOPRE_ODONTO_VAR',
    'RECUP_IPE_COMPL_ODONTO_VAR',
    'RECUP_TOTAL',
    'RECUP_TOTAL_VAR'
]

def calc_pct_change_12(x):
    if len(x) == 12 and x[0] != 0 and not pd.isna(x[-1]):
        return (x[-1] - x[0]) / x[0]
    else:
        return float('nan')

for col_name in colunas:
    df[col_name + '_pct'] = df[col_name].pct_change()
   # df[col_name + '_pct12'] = df[col_name].rolling(window=12).apply(lambda x: (x[-1] - x[0]) / x[0] if len(x) == 12 else None)
    df[col_name + '_pct12'] = df[col_name].rolling(window=12).apply(calc_pct_change_12, raw=True)

df['percentual_CRED_IF_MEDICO_receita']         = df['CRED_IF_MEDICO']            / df['RECEITA_IF_MEDICO'].rolling(12).sum()
df['percentual_CRED_ADESAOPOS_MEDICO_receita']  = df['CRED_ADESAOPOS_MEDICO']     / df['RECEITA_ADESAOPOS_MEDICO'].rolling(12).sum()
df['percentual_CRED_EMP_MEDICO_receita']        = df['CRED_EMP_MEDICO']           / df['RECEITA_EMP_MEDICO'].rolling(12).sum()
df['percentual_CRED_ADESAOPRE_MEDICO_receita']  = df['CRED_ADESAOPRE_MEDICO']     / df['RECEITA_ADESAOPRE_MEDICO'].rolling(12).sum()
df['percentual_CRED_TITULO_ASSOC_receita']      = df['CRED_TITULO_ASSOC']         / df['RECEITA_TITULO_ASSOC'].rolling(12).sum()
df['percentual_CRED_IPE_COMPL_MEDICO_receita']  = df['CRED_IPE_COMPL_MEDICO']     / df['RECEITA_IPE_COMPL'].rolling(12).sum()
    
df['percentual_CRED_IF_ODONTO_receita']         = df['CRED_IF_ODONTO']            / df['RECEITA_IF_ODONTO'].rolling(12).sum()
df['percentual_CRED_ADESAOPOS_ODONTO_receita']  = df['CRED_ADESAOPOS_ODONTO']     / df['RECEITA_ADESAOPOS_ODONTO'].rolling(12).sum()
df['percentual_CRED_EMP_ODONTO_receita']        = df['CRED_EMP_ODONTO']           / df['RECEITA_EMP_ODONTO'].rolling(12).sum()
df['percentual_CRED_ADESAOPRE_ODONTO_receita']  = df['CRED_ADESAOPRE_ODONTO']     / df['RECEITA_ADESAOPRE_ODONTO'].rolling(12).sum()
df['percentual_CRED_IPE_COMPL_ODONTO_receita']  = df['CRED_IPE_COMPL_ODONTO']     / df['RECEITA_IPE_COMPL_ODONTO'].rolling(12).sum()
    
df['percentual_CRED_TOTAL_receita']             = df['CRED_TOTAL']                / df['RECEITA_TOTAL'].rolling(12).sum()

df['percentual_rec_PERDA_IF_MEDICO_PERDAS']         = df['PERDA_IF_MEDICO_PERDAS']          / df['RECEITA_IF_MEDICO'].rolling(12).sum()
df['percentual_rec_PERDA_ADESAOPOS_MEDICO_PERDAS']  = df['PERDA_ADESAOPOS_MEDICO_PERDAS']   / df['RECEITA_ADESAOPOS_MEDICO'].rolling(12).sum()
df['percentual_rec_PERDA_EMP_MEDICO_PERDAS']        = df['PERDA_EMP_MEDICO_PERDAS']         / df['RECEITA_EMP_MEDICO'].rolling(12).sum()
df['percentual_rec_PERDA_ADESAOPRE_MEDICO_PERDAS']  = df['PERDA_ADESAOPRE_MEDICO_PERDAS']   / df['RECEITA_ADESAOPRE_MEDICO'].rolling(12).sum()
df['percentual_rec_PERDA_TITULO_ASSOC_PERDAS']      = df['PERDA_TITULO_ASSOC_PERDAS']       / df['RECEITA_TITULO_ASSOC'].rolling(12).sum()
df['percentual_rec_PERDA_IPE_COMPL_MEDICO']         = df['PERDA_IPE_COMPL_MEDICO']          / df['RECEITA_IPE_COMPL'].rolling(12).sum()

df['percentual_rec_PERDA_IF_ODONTO_PERDAS']         = df['PERDA_IF_ODONTO_PERDAS']          / df['RECEITA_IF_ODONTO'].rolling(12).sum()
df['percentual_rec_PERDA_ADESAOPOS_ODONTO_PERDAS']  = df['PERDA_ADESAOPOS_ODONTO_PERDAS']   / df['RECEITA_ADESAOPOS_ODONTO'].rolling(12).sum()
df['percentual_rec_PERDA_EMP_ODONTO_PERDAS']        = df['PERDA_EMP_ODONTO_PERDAS']         / df['RECEITA_EMP_ODONTO'].rolling(12).sum()
df['percentual_rec_PERDA_ADESAOPRE_ODONTO_PERDAS']  = df['PERDA_ADESAOPRE_ODONTO_PERDAS']   / df['RECEITA_ADESAOPRE_ODONTO'].rolling(12).sum()
df['percentual_rec_PERDA_IPE_COMPL_ODONTO']         = df['PERDA_IPE_COMPL_ODONTO']          / df['RECEITA_IPE_COMPL_ODONTO'].rolling(12).sum()


df['percentual_rec_CRED_IF_MEDICO_VAR'] = df['CRED_IF_MEDICO_VAR']        / df['RECEITA_IF_MEDICO']
df['percentual_rec_CRED_ADESAOPOS_MEDICO_VAR'] = df['CRED_ADESAOPOS_MEDICO_VAR'] / df['RECEITA_ADESAOPOS_MEDICO']
df['percentual_rec_CRED_EMP_MEDICO_VAR'] = df['CRED_EMP_MEDICO_VAR']       / df['RECEITA_EMP_MEDICO']
df['percentual_rec_CRED_ADESAOPRE_MEDICO_VAR'] = df['CRED_ADESAOPRE_MEDICO_VAR'] / df['RECEITA_ADESAOPRE_MEDICO']
df['percentual_rec_CRED_TITULO_ASSOC_VAR'] = df['CRED_TITULO_ASSOC_VAR']     / df['RECEITA_TITULO_ASSOC']
df['percentual_rec_CRED_IPE_COMPL_MEDICO_VAR'] = df['CRED_IPE_COMPL_MEDICO_VAR'] / df['RECEITA_IPE_COMPL']
   
df['percentual_rec_PERDA_IF_MEDICO_PERDAS_VAR'] =    df['PERDA_IF_MEDICO_PERDAS_VAR'       ] / df['RECEITA_IF_MEDICO']     
df['percentual_rec_PERDA_ADESAOPOS_MEDICO_PERDAS_VAR'] =    df['PERDA_ADESAOPOS_MEDICO_PERDAS_VAR'] / df['RECEITA_ADESAOPOS_MEDICO']         
df['percentual_rec_PERDA_EMP_MEDICO_PERDAS_VAR'] =    df['PERDA_EMP_MEDICO_PERDAS_VAR'      ] / df['RECEITA_EMP_MEDICO']     
df['percentual_rec_PERDA_ADESAOPRE_MEDICO_PERDAS_VAR'] =    df['PERDA_ADESAOPRE_MEDICO_PERDAS_VAR'] / df['RECEITA_ADESAOPRE_MEDICO']         
df['percentual_rec_PERDA_TITULO_ASSOC_PERDAS_VAR'] =    df['PERDA_TITULO_ASSOC_PERDAS_VAR'    ] / df['RECEITA_TITULO_ASSOC']     
df['percentual_rec_PERDA_IPE_COMPL_MEDICO_VAR'] =    df['PERDA_IPE_COMPL_MEDICO_VAR'       ] / df['RECEITA_IPE_COMPL']     
    
df['percentual_rec_CRED_IF_ODONTO_VAR']= df['CRED_IF_ODONTO_VAR' ] / df['RECEITA_IF_ODONTO']
df['percentual_rec_CRED_ADESAOPOS_ODONTO_VAR']= df['CRED_ADESAOPOS_ODONTO_VAR' ] / df['RECEITA_ADESAOPOS_ODONTO']
df['percentual_rec_CRED_EMP_ODONTO_VAR']= df['CRED_EMP_ODONTO_VAR' ] / df['RECEITA_EMP_ODONTO']
df['percentual_rec_CRED_ADESAOPRE_ODONTO_VAR']= df['CRED_ADESAOPRE_ODONTO_VAR' ] / df['RECEITA_ADESAOPRE_ODONTO']
df['percentual_rec_CRED_IPE_COMPL_ODONTO_VAR']= df['CRED_IPE_COMPL_ODONTO_VAR' ] / df['RECEITA_IPE_COMPL_ODONTO']
   
df['percentual_rec_PERDA_IF_ODONTO_PERDAS_VAR']= df['PERDA_IF_ODONTO_PERDAS_VAR' ] / df['RECEITA_IF_ODONTO']
df['percentual_rec_PERDA_ADESAOPOS_ODONTO_PERDAS_VAR']= df['PERDA_ADESAOPOS_ODONTO_PERDAS_VAR' ] / df['RECEITA_ADESAOPOS_ODONTO']
df['percentual_rec_PERDA_EMP_ODONTO_PERDAS_VAR']= df['PERDA_EMP_ODONTO_PERDAS_VAR' ] / df['RECEITA_EMP_ODONTO']
df['percentual_rec_PERDA_ADESAOPRE_ODONTO_PERDAS_VAR']= df['PERDA_ADESAOPRE_ODONTO_PERDAS_VAR' ] / df['RECEITA_ADESAOPRE_ODONTO']
df['percentual_rec_PERDA_IPE_COMPL_ODONTO_VAR']= df['PERDA_IPE_COMPL_ODONTO_VAR' ] / df['RECEITA_IPE_COMPL_ODONTO']

df['percentual_rec_RECUP_IF_MEDICO'] = df['RECUP_IF_MEDICO'] /  df['RECEITA_IF_MEDICO']
df['percentual_rec_RECUP_ADESAOPOS_MEDICO'] = df['RECUP_ADESAOPOS_MEDICO'] /  df['RECEITA_ADESAOPOS_MEDICO']
df['percentual_rec_RECUP_EMP_MEDICO'] = df['RECUP_EMP_MEDICO'] /  df['RECEITA_EMP_MEDICO']
df['percentual_rec_RECUP_ADESAOPRE_MEDICO'] = df['RECUP_ADESAOPRE_MEDICO'] /  df['RECEITA_ADESAOPRE_MEDICO']
df['percentual_rec_RECUP_TITULO_ASSOC'] = df['RECUP_TITULO_ASSOC'] /  df['RECEITA_TITULO_ASSOC']
df['percentual_rec_RECUP_IPE_COMPL_MEDICO'] = df['RECUP_IPE_COMPL_MEDICO'] /  df['RECEITA_IPE_COMPL']


df['percentual_rec_RECUP_IF_ODONTO'] = df['RECUP_IF_ODONTO'] / df['RECEITA_IF_ODONTO']
df['percentual_rec_RECUP_ADESAOPOS_ODONTO'] = df['RECUP_ADESAOPOS_ODONTO'] / df['RECEITA_ADESAOPOS_ODONTO']
df['percentual_rec_RECUP_EMP_ODONTO'] = df['RECUP_EMP_ODONTO'] / df['RECEITA_EMP_ODONTO']
df['percentual_rec_RECUP_ADESAOPRE_ODONTO'] = df['RECUP_ADESAOPRE_ODONTO'] / df['RECEITA_ADESAOPRE_ODONTO']
df['percentual_rec_RECUP_IPE_COMPL_ODONTO'] = df['RECUP_IPE_COMPL_ODONTO'] / df['RECEITA_IPE_COMPL_ODONTO']


df['percentual_rec_RECUP_IF_MEDICO_VAR'] = df['RECUP_IF_MEDICO_VAR'] /  df['RECEITA_IF_MEDICO']
df['percentual_rec_RECUP_ADESAOPOS_MEDICO_VAR'] = df['RECUP_ADESAOPOS_MEDICO_VAR'] /  df['RECEITA_ADESAOPOS_MEDICO']
df['percentual_rec_RECUP_EMP_MEDICO_VAR'] = df['RECUP_EMP_MEDICO_VAR'] /  df['RECEITA_EMP_MEDICO']
df['percentual_rec_RECUP_ADESAOPRE_MEDICO_VAR'] = df['RECUP_ADESAOPRE_MEDICO_VAR'] /  df['RECEITA_ADESAOPRE_MEDICO']
df['percentual_rec_RECUP_TITULO_ASSOC_VAR'] = df['RECUP_TITULO_ASSOC_VAR'] /  df['RECEITA_TITULO_ASSOC']
df['percentual_rec_RECUP_IPE_COMPL_MEDICO_VAR'] = df['RECUP_IPE_COMPL_MEDICO_VAR'] /  df['RECEITA_IPE_COMPL']


df['percentual_rec_RECUP_IF_ODONTO_VAR'] = df['RECUP_IF_ODONTO_VAR'] / df['RECEITA_IF_ODONTO']
df['percentual_rec_RECUP_ADESAOPOS_ODONTO_VAR'] = df['RECUP_ADESAOPOS_ODONTO_VAR'] / df['RECEITA_ADESAOPOS_ODONTO']
df['percentual_rec_RECUP_EMP_ODONTO_VAR'] = df['RECUP_EMP_ODONTO_VAR'] / df['RECEITA_EMP_ODONTO']
df['percentual_rec_RECUP_ADESAOPRE_ODONTO_VAR'] = df['RECUP_ADESAOPRE_ODONTO_VAR'] / df['RECEITA_ADESAOPRE_ODONTO']
df['percentual_rec_RECUP_IPE_COMPL_ODONTO_VAR'] = df['RECUP_IPE_COMPL_ODONTO_VAR'] / df['RECEITA_IPE_COMPL_ODONTO']

df['percentual_rec_CRED_TOTAL_receita'] = df['CRED_TOTAL'] / df['RECEITA_TOTAL']
df['percentual_RECUP_TOTAL'] = df['RECUP_TOTAL'] / df['RECEITA_TOTAL']
df['percentual_RECUP_TOTAL_VAR'] = df['RECUP_TOTAL_VAR'] / df['RECEITA_TOTAL']

df['percentual_RECUP_IF_MEDICO'] = df['RECUP_IF_MEDICO'] /  df['RECEITA_IF_MEDICO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPOS_MEDICO'] = df['RECUP_ADESAOPOS_MEDICO'] /  df['RECEITA_ADESAOPOS_MEDICO'].rolling(12).sum()
df['percentual_RECUP_EMP_MEDICO'] = df['RECUP_EMP_MEDICO'] /  df['RECEITA_EMP_MEDICO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPRE_MEDICO'] = df['RECUP_ADESAOPRE_MEDICO'] /  df['RECEITA_ADESAOPRE_MEDICO'].rolling(12).sum()
df['percentual_RECUP_TITULO_ASSOC'] = df['RECUP_TITULO_ASSOC'] /  df['RECEITA_TITULO_ASSOC'].rolling(12).sum()
df['percentual_RECUP_IPE_COMPL_MEDICO'] = df['RECUP_IPE_COMPL_MEDICO'] /  df['RECEITA_IPE_COMPL'].rolling(12).sum()
df['percentual_RECUP_IF_ODONTO'] = df['RECUP_IF_ODONTO'] / df['RECEITA_IF_ODONTO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPOS_ODONTO'] = df['RECUP_ADESAOPOS_ODONTO'] / df['RECEITA_ADESAOPOS_ODONTO'].rolling(12).sum()
df['percentual_RECUP_EMP_ODONTO'] = df['RECUP_EMP_ODONTO'] / df['RECEITA_EMP_ODONTO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPRE_ODONTO'] = df['RECUP_ADESAOPRE_ODONTO'] / df['RECEITA_ADESAOPRE_ODONTO'].rolling(12).sum()
df['percentual_RECUP_IPE_COMPL_ODONTO'] = df['RECUP_IPE_COMPL_ODONTO'] / df['RECEITA_IPE_COMPL_ODONTO'].rolling(12).sum()
df['percentual_RECUP_IF_MEDICO_VAR'] = df['RECUP_IF_MEDICO_VAR'] /  df['RECEITA_IF_MEDICO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPOS_MEDICO_VAR'] = df['RECUP_ADESAOPOS_MEDICO_VAR'] /  df['RECEITA_ADESAOPOS_MEDICO'].rolling(12).sum()
df['percentual_RECUP_EMP_MEDICO_VAR'] = df['RECUP_EMP_MEDICO_VAR'] /  df['RECEITA_EMP_MEDICO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPRE_MEDICO_VAR'] = df['RECUP_ADESAOPRE_MEDICO_VAR'] /  df['RECEITA_ADESAOPRE_MEDICO'].rolling(12).sum()
df['percentual_RECUP_TITULO_ASSOC_VAR'] = df['RECUP_TITULO_ASSOC_VAR'] /  df['RECEITA_TITULO_ASSOC'].rolling(12).sum()
df['percentual_RECUP_IPE_COMPL_MEDICO_VAR'] = df['RECUP_IPE_COMPL_MEDICO_VAR'] /  df['RECEITA_IPE_COMPL'].rolling(12).sum()
df['percentual_RECUP_IF_ODONTO_VAR'] = df['RECUP_IF_ODONTO_VAR'] / df['RECEITA_IF_ODONTO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPOS_ODONTO_VAR'] = df['RECUP_ADESAOPOS_ODONTO_VAR'] / df['RECEITA_ADESAOPOS_ODONTO'].rolling(12).sum()
df['percentual_RECUP_EMP_ODONTO_VAR'] = df['RECUP_EMP_ODONTO_VAR'] / df['RECEITA_EMP_ODONTO'].rolling(12).sum()
df['percentual_RECUP_ADESAOPRE_ODONTO_VAR'] = df['RECUP_ADESAOPRE_ODONTO_VAR'] / df['RECEITA_ADESAOPRE_ODONTO'].rolling(12).sum()
df['percentual_RECUP_IPE_COMPL_ODONTO_VAR'] = df['RECUP_IPE_COMPL_ODONTO_VAR'] / df['RECEITA_IPE_COMPL_ODONTO'].rolling(12).sum()

#df = df.dropna()

df = df.melt(id_vars=['COMPETENCIA'], value_vars=df.columns[1:,])

#%%
output_file = os.path.join(OUTPUT_DIR, f'{nome}.csv')
df.to_csv(output_file, mode='w', index=False, encoding='latin-1', sep=';', decimal=',', date_format="%Y-%m-%d", header=True)

end_time = time.time()
end_time_formatted = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
execution_time = int(end_time - start_time)
print(f"Exportação {nome} encerrada em: {end_time_formatted} - Tempo de execução: {execution_time} segundos")


#### Salvando arquivo
#exportar_csv('pic')