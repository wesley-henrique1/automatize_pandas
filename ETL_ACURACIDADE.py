from config.config_path import directory, outros, wms, relatorios, col_names
from config.fuction import Funcao
import pandas as pd
import numpy as np
import os
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

class acuracidade:
    def __init__(self):
        self.lista_files = [outros.ou_86, wms.wms_07_ger,wms.wms_07_end,relatorios.rel_96]
        print(f"{"ACURACIDADE":_^78}")
        self.carregamento(self.lista_files)
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
    def pipeline(self):
        try:
            df_bloq = pd.read_excel(self.lista_files[0], usecols=['Código', 'Bloqueado(Qt.Bloq.-Qt.Avaria)'])
            end_ger = pd.read_csv(self.lista_files[1], header= None, names= col_names.col_ger)  
            df_end = pd.read_csv(self.lista_files[2], header= None, names= col_names.col_end)
            df_prod = pd.read_excel(self.lista_files[3], usecols= ['CODPROD', 'QTUNITCX','QTTOTPAL'])
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(f"Etapa extração: {self.erro}")
            
        try:        
            dic_end_ger = {'COD' : "CODPROD"}
            end_ger = end_ger.rename(columns= dic_end_ger)
            end_ger['RUA'] = end_ger['RUA'].fillna(0).astype(int)

            dic_bloq = {'Código' : "CODPROD", 'Bloqueado(Qt.Bloq.-Qt.Avaria)' : "BLOQ"}
            df_bloq = df_bloq.rename(columns= dic_bloq)
            df_bloq['CODPROD'] = df_bloq['CODPROD'].fillna(0).astype(int)

            dic_end = {"COD" : "CODPROD"}
            df_end = df_end.rename(columns= dic_end)
            df_end = df_end[['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']]
            
            col_ajuste = ['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']
            for col in col_ajuste:
                df_end = Funcao.ajustar_numero(df_end, col, int)
            
            grupo_end = df_end.groupby('CODPROD').agg(
                SAIDA = ('SAIDA', 'sum'),
                ENTRADA = ('ENTRADA', 'sum'),
                DISP = ('DISP', 'sum'),
                QTDE = ('QTDE', 'sum'),
            ).reset_index()

            df_prod['CODPROD'] = df_prod['CODPROD'].fillna(0).astype(int)

            df = end_ger.loc[end_ger['RUA'].between(1, 39)]
            df = df.merge(df_bloq, left_on='CODPROD',right_on='CODPROD', how= 'left')
            df = df.merge(grupo_end, left_on='CODPROD',right_on='CODPROD', how= 'left')
            df = df.merge(df_prod, left_on='CODPROD',right_on='CODPROD', how= 'left')
            drop_col = ['EMBALAGEM']
            df.drop(columns= drop_col, inplace= True)

            col_ajuste = ['ENDERECO', 'GERENCIAL','PICKING','CAP','QTUNITCX','ENTRADA', 'SAIDA', 'QTDE']
            for col in col_ajuste:
                df = Funcao.ajustar_numero(df, col, float)

            df['DIF_UN'] = df['ENDERECO'].astype(float) - df['GERENCIAL'].astype(float)
            df['DIF_CX'] = (df['DIF_UN'] / df['QTUNITCX'].astype(int)).round(1)
            df['ENDERECO'] = df['ENDERECO'] + df['ENTRADA']
            df['CAP_CONVERTIDA'] = df['CAP'] * df['QTUNITCX']
            df['PENDENCIA'] = np.where(df['QTDE_O.S'] > 0, 'FOLHA', 'INVENTARIO')

            cond_dif = [
                df['DIF_CX'] == 0,
                # POSITIVO
                (df['DIF_CX'] > 0) & (df['DIF_CX'] < 5),
                (df['DIF_CX'] >= 5) & (df['DIF_CX'] < 10),
                df['DIF_CX'] >= 10,
                # NEGATIVO
                (df['DIF_CX'] < 0) & (df['DIF_CX'] > -5),
                (df['DIF_CX'] <= -5) & (df['DIF_CX'] > -10),
                df['DIF_CX'] <= -10
            ]
            result_dif = ['0', '1', '2', '3', '-1', '-2', '-3',]

            cond_op = [
                df['DIF_UN'] < 0,
                df['DIF_UN'] > 0,
                df['DIF_UN'] == 0,
            ]
            result_op = ["END_MENOR", "END_MAIOR", "CORRETO"]

            cond_ap = [
                df['PICKING'].astype(int) > df['CAP_CONVERTIDA'].astype(int),
                df['PICKING'].astype(int) < 0,
                df['PICKING'].astype(int) == 0
            ]
            result_ap = [
                'AP_MAIOR',
                "AP_NEGATIVO",
                "CORRETO"
            ]

            df['CATEGORIA_DIF'] = np.select(cond_dif, result_dif, default= "99").astype(int)
            df["TIPO_OP"] = np.select(cond_op, result_op, default= "-")
            df['AP_VS_CAP'] = np.select(cond_ap, result_ap, default= "-")
            df[['RUA', 'PREDIO']] = df[['RUA', 'PREDIO']].astype(int)
            df_prod = df_prod.drop_duplicates(subset=None, keep='first')
            df = df.sort_values(by=['RUA', 'PREDIO'], ascending= True)
        except Exception as e:
            erro = Funcao.validar_erro(e)
            print(f"Etapa tratamento: {erro}")

        try:
            dic = directory.dir_acuracidade
            path_div = os.path.join(dic, "FATO_DIVERGENCIA.xlsx")
            path_prod = os.path.join(dic, "DIM_PROD.xlsx")

            df.to_excel(path_div, index= False, sheet_name= 'DIVERGENCIA')
            df_prod.to_excel(path_prod, index= False, sheet_name= 'DIM_PROD')
        except Exception as e:
            erro = Funcao.validar_erro(e)
            print(f"Etapa carga: {erro}")

if __name__ == '__main__':
    acuracidade()
    input("Aperte 'enter' para finalizar o processo...")