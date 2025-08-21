import pandas as pd
from BASE import *
import glob
import os
"""
ler todos os arquivos da pasta e agrupar em um df
"""

arquivos= glob.glob(os.path.join(path.p_1782_pasta, '*.txt'))

df = pd.DataFrame([])
for ir ,arquivo in enumerate(arquivos,1):
    x = pd.read_csv(arquivo, header= None)
    df = pd.concat([df, x], axis= 0, ignore_index= True)
df.columns = col.c_1782_sug

# 1. Ler o arquivo
df_85 = pd.read_excel(path.p_8596)
df_85 = df_85.dropna(subset=['RUA'])
df_85 = df_85.loc[df_85['RUA'].between(1, 39)]

with pd.ExcelWriter(var.var_6) as writer:
    df.to_excel(writer,engine='openpyxl', index= False, sheet_name= "depositos")
    df_85.to_excel(writer,engine='openpyxl', index= False, sheet_name= "produto")
    print('fim do processo, verifique o arquivo')


