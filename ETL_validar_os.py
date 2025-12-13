from config.config_path import relatorios, col_names, wms, outros, output
from config.fuction import Funcao
import pandas as pd
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)


class app:
    def __init__(self):
        self.lista_files = [outros.ou_func, wms.wms_07_end, relatorios.rel_28]
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
            col_28 = ['NUMOS','CODPROD', 'DESCRICAO', 'ENDERECO_ORIG', 'RUA', 'PREDIO', 'NIVEL', 'APTO','RUA_1', 'PREDIO_1','NIVEL_1', 'APTO_1', 'QT', 'POSICAO', 'CODFUNCOS', 'CODFUNCESTORNO', 'Tipo O.S.', 'FUNCOSFIM','FUNCGER']
            df_func = pd.read_excel(self.lista_files[0], sheet_name= 'FUNC', usecols= ['ID_FUNC', 'SETOR'])

            df_aereo = pd.read_csv(self.lista_files[1], header= None, names= col_names.col_end)

            df_28 = pd.read_excel(self.lista_files[2], usecols= col_28)
        except Exception as e:
            self.erros = Funcao.validar_erro(e)
            print(F"EXTRAÇÃO: {self.erros}")

        try:
            df_28[['CODFUNCESTORNO', 'CODFUNCOS']] = df_28[['CODFUNCESTORNO', 'CODFUNCOS']].fillna(0).astype(int)
            df_28 = df_28.loc[(df_28['NIVEL'].between(2,8)) & (df_28['RUA'] < 40)  & (df_28['CODFUNCESTORNO'] == 0)  & (df_28['RUA_1'] != 50) & (df_28['Tipo O.S.'] == '58 - Transferencia de Para Vertical')]
            df_28['ID_COD'] = df_28['ENDERECO_ORIG'].astype(str) + " - " + df_28['CODPROD'].astype(str)

            df_aereo = df_aereo.loc[df_aereo['TIPO_PK'] == 'AE']
            df_aereo = df_aereo[['COD_END', 'COD', 'QTDE', 'ENTRADA', 'SAIDA']]
            df_aereo['ID_COD'] = df_aereo['COD_END'].astype(str) + " - " + df_aereo['COD'].astype(str)

            df_baixa = df_28.merge(df_aereo, left_on= 'ID_COD', right_on= 'ID_COD', how= 'left').fillna(0).drop(columns=['COD_END'])
            df_baixa = df_baixa.merge(df_func, left_on= 'CODFUNCOS', right_on= 'ID_FUNC', how= 'left').drop(columns=['ID_FUNC'])
            df_baixa['SETOR'] = df_baixa['SETOR'].fillna("FUNC")  

            col_int = ['QT','QTDE']
            for col in col_int:
                df_baixa = Funcao.ajustar_numero(df_baixa, col, int)

            caso_total = (df_baixa['NIVEL_1'] == 1) & (df_baixa['QT'] >= df_baixa['QTDE'])
            caso_parcial = (df_baixa['NIVEL_1'] == 1) & (df_baixa['QT'] < df_baixa['QTDE'])
            caso_transferencia = (df_baixa['NIVEL_1'] > 1)

            df_baixa['CATEGORIA'] = np.where(caso_total, "TOTAL", 
                                            np.where(caso_parcial,"PARCIAL", 
                                            np.where(caso_transferencia, "TRANSFERENCIA",
                                            "DIVERGENTE")))

            df_pd = df_baixa.loc[(df_baixa['CODFUNCOS'] == 0)].copy()
            df_pd = df_pd[['NUMOS','CODPROD','DESCRICAO','RUA','PREDIO','APTO', 'RUA_1','PREDIO_1','APTO_1','FUNCGER','CATEGORIA','COD']]
            df_pd = df_pd.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)

            col_fim = ['RECEBIMENTO','EMPILHADOR','ABASTECEDOR']
            df_fim = df_baixa.loc[(~df_baixa['SETOR'].isin(col_fim)) & (df_baixa['CODFUNCOS'] > 0) & (df_baixa['QTDE'] == 0)]
            df_fim = df_fim[['NUMOS','CODPROD','DESCRICAO','RUA','PREDIO','APTO','RUA_1','PREDIO_1','APTO_1','FUNCOSFIM','CATEGORIA','COD']]
            df_fim = df_fim.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)
        except Exception as e:
            self.erros = Funcao.validar_erro(e)
            print(F"TRATAMENTO: {self.erros}, {e}")

        try:
            with pd.ExcelWriter(output.rel_os) as destino:
                df_pd.to_excel(destino, index= False, sheet_name= 'PEDENTES')
                df_fim.to_excel(destino, index= False, sheet_name= 'FIM_MESA')
        except Exception as e:
            self.erros = Funcao.validar_erro(e)
            print(F"CARGA: {self.erros}")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")