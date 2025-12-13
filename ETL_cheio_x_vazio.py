from config.config_path import *
from config.fuction import Funcao
import pandas as pd
import glob
import os

class cheio_vazio:
    def __init__(self):
        self.list_dir = [directory.dir_cheio_vazio]
        pass

    def carregamento(self):
        pass

    def pipeline(self):         
        try:
            pasta_files = glob.glob(os.path.join(directory.dir_cheio_vazio, "*xls*"))
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(f"Extração: {self.erro}\n")

        try:
            list_processado = []
            list_erros = {}

            for file in pasta_files:
                NAME_FILE = os.path.basename(file)
                try:
                    df = pd.read_excel(file, header= 2)
                    df_filtrado = df.loc[df['Retorno'].isin(['CHEIO', 'VAZIO'])]
                    df_filtrado = df_filtrado[['END', 'COD', 'DESCRIÇÃO', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'Retorno', 'data_relatorio']]
                    df_filtrado['NAME_FILE'] = NAME_FILE
                    list_processado.append(df_filtrado)
                except Exception as e:
                    list_erros['ETAPA_CONSOLIDADO'] = str(NAME_FILE), str(e)
                
            df_consolidado = pd.concat(list_processado, axis= 'index')

            col_int = ['END', 'COD','RUA','PREDIO','NIVEL','APTO']
            for col in col_int:
                try:
                    df_consolidado[col] = df_consolidado[col].fillna(0).astype(int)
                except Exception as e:
                    list_erros.clear()
                    list_erros[col] = str(e)
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(f"Tratamento: {self.erro}\n")

        try:
            pass
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(f"Carga: {self.erro}\n")

if __name__ == "__main__":
    cheio_vazio()
    input("\nPressione Enter para sair...")
