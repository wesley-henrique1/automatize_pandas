import pandas as pd
import BASE as bs
import glob
import os
"""
ler todos os arquivos da pasta e agrupar em um df
"""

arquivos= glob.glob(os.path.join(bs.path.p_1782_pasta, '*.xls'))
total = len(arquivos)

df = pd.DataFrame(columns= bs.col.c_1782_sug)

for ir ,arquivo in enumerate(arquivos,1):
    df_a = pd.read_excel(arquivo, header= None)
    df = pd.concat([df, df_a], axis= 0, ignore_index= True)
    
    x = df.groupby(by= 'DEP').agg(
        qtde_itens = ('DESCRIÇÃO', 'count')
    ).reset_index()
