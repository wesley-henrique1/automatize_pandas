from config.config_path import directory, relatorios, outros, col_names
from config.fuction import Funcao
import pandas as pd
import numpy as np
import warnings
import os
warnings.simplefilter(action='ignore', category=UserWarning)


class Acesso:
    def __init__(self):
        self.lista_df = [directory.dir_82, relatorios.rel_96, relatorios.rel_60, outros.ou_86, directory.dir_acesso]

        print(f"{"Acesso":_^78}")
        self.carregamento(self.lista_df)
        self.pipeline()

    def carregamento(self, lista_files):
        try:
            print(f"\n{"ARQUIVOS":_^78}")
            for i, path in enumerate(lista_files):
                self.validar = Funcao.leitura(path, i+1)
            print(f"{"_" * 78}")
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(self.erro)
            exit()
    def pipeline(self):
        try: #"""LEITURA DOS DATAFRAMES"""
            df_sug = Funcao.director(self.lista_df[0], 'DEP*.txt')
            df_mov = Funcao.director(self.lista_df[0], 'MOV*.txt')
            df_prod = pd.read_excel(self.lista_df[1], usecols=['CODPROD','DESCRICAO', 'QTUNITCX', 'QTTOTPAL', 'OBS2','RUA', 'PREDIO', 'APTO','EMBALAGEM','CAPACIDADE'])
            df_acesso = pd.read_excel(self.lista_df[2], usecols= ['CODPROD','QTOS', 'QT'])
            df_estoque = pd.read_excel(self.lista_df[3], usecols=['Código', 'Estoque', 'Custo ult. ent.','Qtde Pedida','Bloqueado(Qt.Bloq.-Qt.Avaria)','Qt.Avaria'])
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print("EXTRAÇÃO: \n", self.erro)

        try:# """TRATATIVAS DOS DF"""
            df_sug.columns = col_names.col_sug
            df_sug = df_sug.drop_duplicates(subset=['COD'], keep='first')
            df_sug.replace([np.inf, -np.inf], 0, inplace=True) 
            df_sug = df_sug.drop(columns= ['EMBALAGEM','DESCRIÇÃO','UNID.','DEP', 'RUA', 'PREDIO', 'NIVEL', 'APTO','TIPO', 'CAP'])
            
            df_mov.columns = col_names.col_mov
            df_mov = df_mov.drop_duplicates(subset=['COD'], keep='first')
            df_mov.replace([np.inf, -np.inf], 0, inplace=True) 
            df_mov = df_mov.drop(columns= ['DESC', 'EMBALAGEM', 'UNID'])

            print(f"df_sug: {df_sug.columns}\n")
            print(f"df_mov: {df_mov.columns}\n")


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

            df[['COM FATOR', 'QTTOTPAL', 'CAPACIDADE']] = df[['COM FATOR', 'QTTOTPAL','CAPACIDADE']].fillna(0)
            df['PL_SUG'] = (df['COM FATOR'].astype(int) / df['QTTOTPAL'].astype(int)).fillna(0).round(2)
            df['PL_CAP'] = (df['CAPACIDADE'].astype(int) / df['QTTOTPAL'].astype(int)).fillna(0).round(2)
            df['Custo ult. ent.'] = df['Custo ult. ent.'].astype(float).fillna(0).round(2)
            df['CUSTO_TT'] = (df['Estoque'].astype(float) * df['Custo ult. ent.']).fillna(0).round(2)
            df['CONTAGEM'] = concat.map(concat.value_counts())
            df['STATUS_PROD'] = np.where(df['CONTAGEM'] > 3, "DIV", np.where(df['CONTAGEM'] > 2,"VAL", "INT"))
            df.replace([np.inf, -np.inf], 0, inplace=True) 
            
            drop_col = [ '%_x','COD_y',  'COD_x','Código', 'ACESSO','%_y', '%_ACUM','NIVEL','PEDIDO_COMP', 'BLOQUEADO','AVARIA']
            df_final = df.drop(columns=drop_col)

            dic_rename = {
                "EMBALAGEM_y" : "EMB"
                ,"RUA_x" : "RUA"
                ,"PREDIO_x" : "PREDIO"
                ,"APTO_x" : "APTO"
                ,"Custo ult. ent." : "CUSTO_ULT"
            }
            df_final = df_final.rename(columns=dic_rename)

            nova_ordem = ['CODPROD', 'DESC','EMB', 'OBS2', 'QTUNITCX','QTTOTPAL', 'RUA', 'PREDIO', 'APTO', 'TIPO', 'MÊS 1', 'MÊS 2', 'MÊS 3', 'CAP', '1 DIA', 'COM FATOR', 'VARIAÇÃO','Estoque', 'CUSTO_ULT','MOVI', 'CLASSE', 'PL_SUG', 'PL_CAP', 'CONTAGEM', 'STATUS_PROD', 'CUSTO_TT','PRODUTO']
            df_final = df_final[nova_ordem]
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print("TRATAMENTO: \n", self.erro)  

        try:# """ETAPA FINAL SALVAR OS DATAFRAMES"""
            pass
            dir = self.lista_df[4]
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
            self.erro = Funcao.validar_erro(e)
            print("CARGA: \n", self.erro)

if __name__ == "__main__":
    Acesso()
    input("\nPressione Enter para sair...")