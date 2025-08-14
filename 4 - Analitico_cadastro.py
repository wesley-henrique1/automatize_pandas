import pandas as pd
import numpy as np
import BASE as bs

try:
    print("ANALITICO CADASTRO")
    print("=" * 70)
    print("\n iniciando as tratativa da base de dados...")
    df_96 = pd.read_excel(bs.path.p_8596, usecols= ['CODPROD', 'DESCRICAO', 'OBS2', 'QTUNITCX', 'LASTROPAL', 'ALTURAPAL', 'QTTOTPAL','ALTURAARM', 'LARGURAARM', 'COMPRIMENTOARM', 'CAPACIDADE', 'ABASTECEPALETE', 'RUA', 'PREDIO', 'NIVEL', 'APTO'])

except PermissionError as e:
    print(f"Arquivos aberto, favor fechar. {e}")
try:
    colunas_int = ['CODPROD','QTUNITCX', 'LASTROPAL', 'QTTOTPAL', 'ALTURAPAL', 'CAPACIDADE']
    colunas_float = [ 'ALTURAARM', 'LARGURAARM', 'COMPRIMENTOARM']
    df_96[colunas_int] = df_96[colunas_int].fillna(0).astype(int)  
    df_96[colunas_float] = df_96[colunas_float].fillna(0).astype(float) 
    df_96 = df_96[df_96["RUA"].between(1,39)]
    df_96['CAP_CONVERTIDA'] = df_96['CAPACIDADE'] * df_96['QTUNITCX']
    var = df_96['RUA'].astype(str) + "-" + df_96['PREDIO'].astype(str)
    df_96['CONTAGEM'] = var.map(var.value_counts())
    df_96['SITUAÇÃO_PROD'] = np.where(df_96['CONTAGEM'] > 3, "DIV", np.where(df_96['CONTAGEM'] > 2,"VAL", "INT"))
    x = (df_96['ALTURAARM'] * df_96['ALTURAPAL']).fillna(0).astype(int)
    x_limpo = x.fillna(0)
    altura_arm_limpa = df_96['ALTURAARM'].fillna(0)
    df_96['P+L?']= np.where(altura_arm_limpa == 0,0,(167 - x_limpo) / altura_arm_limpa).astype(int)

    df_96.to_excel(bs.dest.destino_3, index= False, sheet_name= "validação_PROD")
    print("Processo finalizado com sucesso...")
except Exception as e:
    print(f"Erro na tratativa do dataFrame 8596")


