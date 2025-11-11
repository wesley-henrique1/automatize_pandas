from config.config_path import *
import pandas as pd
import numpy as np
import glob
import os

print("Iniciando processo, favor aguarde...\n")
print("=" * 60)
def director(files, argumento):
    director = glob.glob(os.path.join(files, argumento))
    lista = []
    for arquivo in director:
        x = pd.read_csv(arquivo, header= None)
        lista.append(x)
    df_temp = pd.concat(lista, axis= 0, ignore_index= True)
    return df_temp
def validar_erro(e):
    print("=" * 60)
    if isinstance(e, KeyError):
        return f"KeyError: A coluna ou chave '{e}' não foi encontrada."
    elif isinstance(e, PermissionError):
        return "PermissionError: O arquivo está sendo usado ou você não tem permissão para acessá-lo. Por favor, feche o arquivo."
    elif isinstance(e, TypeError):
        return f"TypeError: Erro de tipo. Verifique se os dados são do tipo correto. Mensagem original: {e}"
    elif isinstance(e, ValueError):
        return f"ValueError: Erro de valor. Mensagem original: {e}"
    else:
        return f"Ocorreu um erro inesperado: {e}"

def app():
    try: #"""LEITURA DOS DATAFRAMES"""

        df_sug = director(directory.dir_82, 'DEP*.txt')
        df_mov = director(directory.dir_82, 'MOV*.txt')
        df_prod = pd.read_excel(relatorios.rel_96, usecols=['CODPROD','DESCRICAO', 'QTUNITCX', 'QTTOTPAL', 'OBS2','RUA', 'PREDIO', 'APTO'])
        df_acesso = pd.read_excel(relatorios.rel_60, usecols= ['CODPROD','QTOS', 'QT'])
        df_estoque = pd.read_excel(outros.ou_86, usecols=['Código', 'Estoque', 'Custo ult. ent.','Qtde Pedida','Bloqueado(Qt.Bloq.-Qt.Avaria)','Qt.Avaria'])
        print("\nleitura dos DATAFRAMES finalizado...\n")
    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA 1: \n", erro)

    try:# """TRATATIVAS DOS DF"""
        df_sug.columns = col_names.col_sug
        df_sug = df_sug.drop_duplicates(subset=['COD'], keep='first')
        df_sug.replace([np.inf, -np.inf], 0, inplace=True) 

        df_mov.columns = col_names.col_mov
        df_mov = df_mov.drop_duplicates(subset=['COD'], keep='first')
        df_mov.replace([np.inf, -np.inf], 0, inplace=True) 

        df_prod = df_prod.loc[df_prod['RUA'].between(1,39)]
        df_prod['OBS2'] = df_prod['OBS2'].fillna("Ativos")
        df_prod["PRODUTO"] = df_prod['CODPROD'].astype(str)

        df_estoque = df_estoque.fillna(0)
        rename = {
            "Qtde Pedida" : "PEDIDO_COMP"
            ,"Bloqueado(Qt.Bloq.-Qt.Avaria)" : "BLOQUEADO"
            ,"Qt.Avaria" : "AVARIA"
        }
        df_estoque = df_estoque.rename(columns=rename)

        df_acesso = df_acesso.groupby('CODPROD').agg(
        ACESSO = ("QTOS", "sum")
        ).reset_index()

        df = df_prod.merge(df_sug, left_on= 'CODPROD', right_on= 'COD', how= 'left')
        df = df.merge(df_estoque, left_on='CODPROD', right_on='Código', how='left')
        df = df.merge(df_acesso, left_on='CODPROD', right_on='CODPROD', how= 'left')
        df = df.merge(df_mov, left_on='CODPROD', right_on='COD', how="left")

        concat = df['RUA_x'].astype(str) + "-" + df['PREDIO_x'].astype(str)

        df[['COM FATOR', 'QTTOTPAL', 'CAP']] = df[['COM FATOR', 'QTTOTPAL','CAP']].fillna(0)
        df['PL_SUG'] = (df['COM FATOR'].astype(int) / df['QTTOTPAL'].astype(int)).fillna(0).round(2)
        df['PL_CAP'] = (df['CAP'].astype(int) / df['QTTOTPAL'].astype(int)).fillna(0).round(2)
        df['Custo ult. ent.'] = df['Custo ult. ent.'].astype(float).fillna(0).round(2)
        df['CUSTO_TT'] = (df['Estoque'].astype(float) * df['Custo ult. ent.']).fillna(0).round(2)
        df['CONTAGEM'] = concat.map(concat.value_counts())
        df['STATUS_PROD'] = np.where(df['CONTAGEM'] > 3, "DIV", np.where(df['CONTAGEM'] > 2,"VAL", "INT"))
        df.replace([np.inf, -np.inf], 0, inplace=True) 

        drop_col = ['UNID.','DESCRIÇÃO', '%_x',  'COD','RUA_y', 'PREDIO_y', 'APTO_y','Código', 'ACESSO','DESCRICAO','EMBALAGEM_y', 'UNID','%_y', '%_ACUM','NIVEL','PEDIDO_COMP', 'BLOQUEADO','AVARIA']
        df_final = df.drop(columns=drop_col)

        dic_rename = {
            "EMBALAGEM_x" : "EMB"
            ,"RUA_x" : "RUA"
            ,"PREDIO_x" : "PREDIO"
            ,"APTO_x" : "APTO"
            ,"Custo ult. ent." : "CUSTO_ULT"
        }
        df_final = df_final.rename(columns=dic_rename)

        nova_ordem = ['COD', 'DESC','EMB', 'OBS2', 'QTUNITCX','QTTOTPAL', 'RUA', 'PREDIO', 'APTO', 'TIPO', 'MÊS 1', 'MÊS 2', 'MÊS 3', 'CAP', '1 DIA', 'COM FATOR', 'VARIAÇÃO','Estoque', 'CUSTO_ULT','MOVI', 'CLASSE', 'PL_SUG', 'PL_CAP', 'CONTAGEM', 'STATUS_PROD', 'CUSTO_TT','PRODUTO']
        df_final = df_final[nova_ordem]
        print("ratamento finalizado...\n")
    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA 2: \n", erro)
        

    try:# """ETAPA FINAL SALVAR OS DATAFRAMES"""
        dir = directory.dir_acesso
        file_sug = os.path.join(dir, "DIM_SUGESTÃO.xlsx")
        file_mov = os.path.join(dir, "DIM_MOVIMENTAR.xlsx")
        file_prod = os.path.join(dir, "DIM_PRODUTO.xlsx")
        file_acesso = os.path.join(dir, "DIM_ACESSO.xlsx")
        file_estoque = os.path.join(dir, "DIM_ESTOQUE.xlsx")
        file_final = os.path.join(dir, "FATO_GERAL.xlsx")

        df_sug.to_excel(file_sug, index=False, sheet_name="DIM_SUG")
        df_mov.to_excel(file_mov, index=False, sheet_name="DIM_MOVI")  
        df_prod.to_excel(file_prod, index=False, sheet_name="DIM_PROD")  
        df_acesso.to_excel(file_acesso, index=False, sheet_name="DIM_ACESSO")
        df_estoque.to_excel(file_estoque, index=False, sheet_name="DIM_GER")  
        df_final.to_excel(file_final, index=False, sheet_name="FATO_GERAL")  



    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA FINAL: \n", erro)

if __name__ == "__main__":
    app = app()
    print("Processo finalizado com sucesso!!...\n")
    input("\nPressione Enter para sair...")