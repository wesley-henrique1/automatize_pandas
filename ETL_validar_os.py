from OUTROS.path_arquivos import *
import pandas as pd

try:
    df_28 = pd.read_excel(ar_xlsx.ar_28, usecols= ['CODPROD', 'DESCRICAO', 'ENDERECO_ORIG', 'RUA', 'PREDIO', 'NIVEL', 'APTO','RUA_1', 'PREDIO_1', 'NIVEL_1', 'APTO_1', 'QT', 'POSICAO', 'CODFUNCOS', 'CODFUNCESTORNO', 'Tipo O.S.'])
    df_28 = df_28.loc[(df_28['NIVEL'].between(2,8)) & (df_28['Tipo O.S.'] == '58 - Transferencia de Para Vertical')]
    df_28[['CODFUNCESTORNO', 'CODFUNCOS']] = df_28[['CODFUNCESTORNO', 'CODFUNCOS']].fillna(0).astype(int)
    df_28['ID_COD'] = df_28['ENDERECO_ORIG'].astype(str) + " - " + df_28['CODPROD'].astype(str)

    df_aereo = pd.read_csv(ar_csv.ar_end, header= None, names= col_name.cEnd)
    df_aereo = df_aereo[['COD_END', 'COD', 'QTDE', 'ENTRADA', 'SAIDA']]
    df_aereo['ID_COD'] = df_aereo['COD_END'].astype(str) + " - " + df_aereo['COD'].astype(str)

    df_func = pd.read_excel(ar_xlsx.ar_func, sheet_name= 'FUNCIONARIOS')
    df = df_28.merge(df_aereo, left_on= 'ID_COD', right_on= 'ID_COD', how= 'left').fillna(0).drop(['COD_END', 'COD'],axis= 1)
except Exception as e:
    print(f"Erro na leitura dos dataframes\n{e}")

try:
    
    print(df)
    pass
except Exception as e:
    pass





# with pd.ExcelWriter(output.val_baixa) as writer:
#     .to_excel(writer, index= False, sheet_name= 'PEDENTES')
#     .to_excel(writer, index= False, sheet_name= 'FIM_MESA')