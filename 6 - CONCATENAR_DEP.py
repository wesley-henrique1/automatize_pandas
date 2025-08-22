from path_arquivos import *
import pandas as pd
import glob
import os
"""
ler todos os arquivos da pasta e agrupar em um df
"""

arquivos= glob.glob(os.path.join(pasta.p_82, 'DEP*.txt'))

df = pd.DataFrame([])
for ir ,arquivo in enumerate(arquivos,1):
    x = pd.read_csv(arquivo, header= None)
    df = pd.concat([df, x], axis= 0, ignore_index= True)
df.columns = col_name.c82


with pd.ExcelWriter(output.dep_82) as writer:
    df.to_excel(writer,engine='openpyxl', index= False, sheet_name= "depositos")
    print('fim do processo, verifique o arquivo')


