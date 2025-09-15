from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np
import glob
import os

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
        def directory(files, argumento):
            directory = glob.glob(os.path.join(files, argumento))
            lista = []
            for arquivo in directory:
                x = pd.read_csv(arquivo, header= None)
                lista.append(x)
            df_temp = pd.concat(lista, axis= 0, ignore_index= True)
            return df_temp
        
        df_sug = directory(pasta.p_82, 'DEP*.txt')
        df_mov = directory(pasta.p_82, 'MOV*.txt')
        df_prod = pd.read_excel(ar_xlsx.ar_96, usecols=['CODPROD','DESCRICAO', 'QTUNITCX', 'QTTOTPAL', 'OBS2','RUA', 'PREDIO', 'APTO'])
        df_acesso = pd.read_excel(ar_xlsx.ar_60, usecols= ['CODPROD','QTOS', 'QT'])

    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA 1: \n", erro)

    try:# """TRATATIVAS DOS DF"""
        df_sug.columns = col_name.c82
        df_mov.columns = col_name.cMov

        df_prod = df_prod.loc[df_prod['RUA'].between(1,39)]
        df_prod['OBS2'] = df_prod['OBS2'].fillna("Ativos")
        df_prod["PROD"] = df_prod['CODPROD'].astype(str)

        df_acesso = df_acesso.groupby('CODPROD').agg(
            ACESSO = ("QTOS", "sum")
        ).reset_index()

        df = df_sug.merge(df_prod, left_on= 'COD', right_on= 'CODPROD', how= 'left')

    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA 2: \n", erro)

    try:# """ETAPA FINAL SALVAR OS DATAFRAMES"""
        directory = files_bi.bi_acesso
        file_sug = os.path.join(directory, "DIM_SUGESTÃO.xlsx")
        file_mov = os.path.join(directory, "DIM_MOVIMENTAR.xlsx")
        file_prod = os.path.join(directory, "DIM_PRODUTO.xlsx")

        df_sug.to_excel(file_sug, index=False, sheet_name="DIM_SUG")
        df_mov.to_excel(file_mov, index=False, sheet_name="DIM_MOVI")  
        df_prod.to_excel(file_prod, index=False, sheet_name="DIM_PROD")  

    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA FINAL: \n", erro)