from path_arquivos import *
import pandas as pd

df = pd.read_excel(ar_xlsx.ar_28)
df_func = pd.read_excel(ar_xlsx.ar_func, sheet_name= 'FUNCIONARIOS')

y = df.loc[df['NIVEL'].between(2,8)].copy()

y = y.loc[(y['POSICAO'] == 'P') & (y['Tipo O.S.'] == '58 - Transferencia de Para Vertical')]

x = y.merge(df_func, how= 'left', left_on='CODFUNCOS', right_on='MATRICULA')
x['TIPO'] = x['TIPO'].fillna(value= 'func')
x = x.drop('MATRICULA', axis= 1)

x = x.loc[(x['TIPO'] != 'ABASTECEDOR') & (x['TIPO'] != 'EMPILHADOR')]

with pd.ExcelWriter(output.val_baixa) as writer:
    y.to_excel(writer, index= False, sheet_name= 'PEDENTES')
    x.to_excel(writer, index= False, sheet_name= 'FIM_MESA')