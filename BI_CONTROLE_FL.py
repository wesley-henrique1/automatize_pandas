from config.config_path import relatorios, outros, output
from config.fuction import Funcao
import pandas as pd
import numpy as np
import datetime
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)


class controle_FL:
    def __init__(self):
        self.lista_df = [relatorios.rel_96, outros.ou_86]
        

        self.HOJE = pd.to_datetime(datetime.date.today())

        print(f"{f"controle_FL | {self.HOJE:%d/%m/%Y}":_^78}")
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
        try:
            col_96 = ['CODPROD', 'DESCRICAO','OBS2', 'RUA', 'PREDIO', 'APTO', 'DTULTENT']
            df_8596 = pd.read_excel(self.lista_df[0], usecols= col_96)

            col_86 = ['Código', 'Estoque', 'Qtde Pedida', 'Bloqueado(Qt.Bloq.-Qt.Avaria)', 'Qt.Avaria', 'Custo real', 'Disponível']
            df_286 = pd.read_excel(self.lista_df[1], usecols= col_86)
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(self.erro)
            
        try:
            df = df_8596.merge(df_286, left_on= 'CODPROD', right_on= 'Código', how= 'inner')
            df = df[df['RUA'].between(1, 40)]
            df['BLOQ + AV'] = df['Bloqueado(Qt.Bloq.-Qt.Avaria)'] + df['Qt.Avaria']
            df['BLOQ?'] = np.where(df['BLOQ + AV'] >= df['Estoque'], 'S', 'N')
            df['DTULTENT'] = pd.to_datetime(df['DTULTENT']).dt.normalize()
            df['total_custo'] = df['Custo real'] * df['Disponível']

            df['DIAS_PD'] = self.HOJE - df['DTULTENT']
            df.drop(columns=["Código", "Bloqueado(Qt.Bloq.-Qt.Avaria)", "Qt.Avaria"])

            fl_ativos = df.loc[(df['OBS2'] == 'FL') & (df['Estoque'] > 0) & (df['BLOQ?'] == "N")]
            fl_inativo = df.loc[(df['OBS2'] == 'FL') & (df['Estoque'] == 0)]
            ativos_zerados = df.loc[(df['OBS2'] != 'FL') & (df['Estoque'] == 0) & (df['Qtde Pedida'] == 0) & (df['DIAS_PD'] >= pd.Timedelta(days= 30))]
        except Exception as e: 
            self.erro = Funcao.validar_erro(e)
            print(self.erro)

        try:
            with pd.ExcelWriter(output.controle_fl) as feijao:
                fl_ativos.to_excel(feijao, sheet_name= "FL_ATIVOS", index= False)
                fl_inativo.to_excel(feijao, sheet_name= "FL_END", index= False)
                ativos_zerados.to_excel(feijao, sheet_name= "INATIVOS", index= False)
                df.to_excel(feijao, sheet_name= "GERAL", index= False)
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(self.erro)

if __name__ == "__main__":
    controle_FL()
    input("\nPressione Enter para sair...")