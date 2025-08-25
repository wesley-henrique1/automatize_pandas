from path_arquivos import *
import pandas as pd

df = pd.read_excel(ar_xlsx.ar_28)
df_func = pd.read_excel(ar_xlsx.ar_func, sheet_name= 'FUNCIONARIOS')

df = df.loc[df['NIVEL'].between(2,8)].copy()

df_pd = df.loc[(df['POSICAO'] == 'P') & (df['Tipo O.S.'] == '58 - Transferencia de Para Vertical')]

df_fim_mesa = df.merge(df_func, how= 'left', left_on='CODFUNCOS', right_on='MATRICULA')
df_fim_mesa['TIPO'] = df_fim_mesa['TIPO'].fillna(value= 'func')
df_fim_mesa = df_fim_mesa.drop('MATRICULA', axis= 1)

df_fim_mesa = df_fim_mesa.loc[(df_fim_mesa['TIPO'] != 'ABASTECEDOR') & (df_fim_mesa['TIPO'] != 'EMPILHADOR')]

col_pd = ['CODPROD', 'DESCRICAO', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'RUA_1', 'PREDIO_1', 'NIVEL_1','APTO_1','POSICAO', 'FUNCGER']
col_fim = ['CODPROD', 'DESCRICAO', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'RUA_1', 'PREDIO_1', 'NIVEL_1','APTO_1', 'FUNCOSFIM']

df_fim_mesa.drop(~col_fim, axis= 0)
with pd.ExcelWriter(output.val_baixa) as writer:
    df_pd.to_excel(writer, index= False, sheet_name= 'PEDENTES')
    df_fim_mesa.to_excel(writer, index= False, sheet_name= 'FIM_MESA')