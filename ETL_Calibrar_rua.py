from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np
import glob
import os

print("Iniciando processo, favor aguarde...")
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
        df_estoque = pd.read_excel(ar_xls.ar_86, usecols=['Código', 'Estoque', 'Custo ult. ent.'])

    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA 1: \n", erro)

    try:# """TRATATIVAS DOS DF"""
        
        df_sug.columns = col_name.c82
        df_mov.columns = col_name.cMovi

        df_prod = df_prod.loc[df_prod['RUA'].between(1,39)]
        df_prod['OBS2'] = df_prod['OBS2'].fillna("Ativos")
        df_prod["PRODUTO"] = df_prod['CODPROD'].astype(str)

        df_estoque = df_estoque.fillna(0)

        df_acesso = df_acesso.groupby('CODPROD').agg(
        ACESSO = ("QTOS", "sum")
        ).reset_index()

        df = df_sug.merge(df_prod, left_on= 'COD', right_on= 'CODPROD', how= 'left')
        df = df.merge(df_estoque, left_on='COD', right_on='Código', how='left')
        df = df.merge(df_acesso, left_on='COD', right_on='CODPROD', how= 'left')
        df = df.merge(df_mov, left_on='COD', right_on='COD', how="left")

        concat = df['RUA_x'].astype(str) + "-" + df['PREDIO_x'].astype(str)

        df[['COM FATOR', 'QTTOTPAL', 'CAP']] = df[['COM FATOR', 'QTTOTPAL','CAP']].fillna(0)
        df['PL_SUG'] = (df['COM FATOR'].astype(int) / df['QTTOTPAL'].astype(int)).round(2)
        df['PL_CAP'] = (df['CAP'].astype(int) / df['QTTOTPAL'].astype(int)).round(2)
        df['Custo ult. ent.'] = df['Custo ult. ent.'].astype(float).round(2)
        df['CUSTO_TT'] = (df['Estoque'].astype(float) * df['Custo ult. ent.']).round(2)
        df['CONTAGEM'] = concat.map(concat.value_counts())
        df['STATUS_PROD'] = np.where(df['CONTAGEM'] > 3, "DIV", np.where(df['CONTAGEM'] > 2,"VAL", "INT"))

        drop_col = ['UNID.','DESCRIÇÃO', '%_x',  'CODPROD_x','RUA_y', 'PREDIO_y', 'APTO_y','Código', 'CODPROD_y', 'ACESSO','DESCRICAO','EMBALAGEM_y', 'UNID','%_y', '%_ACUM','NIVEL']
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

    except Exception as e:
        erro = validar_erro(e)
        print("ETAPA 2: \n", erro)

    try:# """ETAPA FINAL SALVAR OS DATAFRAMES"""
        directory = files_bi.bi_acesso
        file_sug = os.path.join(directory, "DIM_SUGESTÃO.xlsx")
        file_mov = os.path.join(directory, "DIM_MOVIMENTAR.xlsx")
        file_prod = os.path.join(directory, "DIM_PRODUTO.xlsx")
        file_acesso = os.path.join(directory, "DIM_ACESSO.xlsx")
        file_estoque = os.path.join(directory, "DIM_ESTOQUE.xlsx")
        file_final = os.path.join(directory, "FATO_GERAL.xlsx")

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