from OUTROS.path_arquivos import *
import pandas as pd

df = pd.read_excel(ar_xlsx.ar_96)


df_filtro = df.loc[df['RUA'].between(1, 39)].copy()
df_filtro['DTULTENT'] = pd.to_datetime(df_filtro['DTULTENT'], errors='coerce')
df_filtro['DTULTENT'] = df_filtro['DTULTENT'].fillna(value='1900-01-01')
df_filtro.to_excel(output.filtro_96, index= False, sheet_name= 'filtrado')