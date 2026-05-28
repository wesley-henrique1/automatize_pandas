from ..lib.settings import Relatorios, ColNames, Wms, Outros, Output
from ..lib import ValidarErros, MonitorETL

import datetime as dt
import pandas as pd
import numpy as np
import os

class auxiliar:
    def ajustar_numero(self, df_copia, coluna, tipo_dados):
        def ajustar(valor):
            if pd.isna(valor) or valor is None:
                return 0.0
            valor = str(valor).strip()
            if isinstance(valor, str):
                if valor.count('.') >= 1 and ',' in valor: 
                    valor = valor.replace('.', '').replace(',', '.')
                elif ',' in valor:
                    valor = valor.replace(',', '.')
            try:
                if tipo_dados == int:
                    return int(float(valor))
                elif tipo_dados == float:
                    return round(float(valor), 2)
            except (ValueError, TypeError):
                return 0.0
        df_copia[coluna] = df_copia[coluna].apply(ajustar)
        return df_copia
class OSCheck(auxiliar):
    validador = ValidarErros(fonte="OSCheck")
    def __init__(self):
        self.list_path = [Outros.ou_func, Wms.wms_07_end, Relatorios.rel_28]
        self.Retorno = [Output.rel_os]
        hoje = dt.datetime.now()
        self.ontem =hoje - dt.timedelta(days=1)
        self.ontem = self.ontem.date()
        self.Instancia = MonitorETL()

    def pipeline(self):
        try:
            self.Instancia.stageTime('Extract')
            col_28 = ['DATA','NUMOS','CODPROD', 'DESCRICAO', 'ENDERECO_ORIG', 'RUA', 'PREDIO', 'NIVEL', 'APTO','RUA_1', 'PREDIO_1','NIVEL_1', 'APTO_1', 'QT', 'POSICAO', 'CODFUNCOS', 'CODFUNCESTORNO', 'Tipo O.S.', 'FUNCOSFIM','FUNCGER']

            df_func = pd.read_excel(self.list_path[0], sheet_name= 'ATIVOS', usecols= ['ID_FUNC', 'TIPO_ABST'])
            df_aereo = pd.read_csv(self.list_path[1], header= None, names= ColNames.col_end)
            df_28 = pd.read_excel(self.list_path[2], usecols= col_28)
            self.Instancia.stageTime('Extract')
        except Exception as e:
            self.validador.registrar_log(e, "Extract")
            return False
        try:
            self.Instancia.stageTime('Transform')
            df_28['DATA'] = pd.to_datetime(df_28['DATA'], dayfirst= True)
            df_28 = df_28.loc[df_28['DATA'].dt.date == self.ontem]
            
            df_28[['CODFUNCESTORNO', 'CODFUNCOS']] = df_28[['CODFUNCESTORNO', 'CODFUNCOS']].fillna(0).astype(int)
            df_28 = df_28.loc[(df_28['NIVEL'].between(2,8)) & (df_28['RUA'] < 40)  & (df_28['CODFUNCESTORNO'] == 0)  & (df_28['RUA_1'] != 50) & (df_28['Tipo O.S.'] == '58 - Transferencia de Para Vertical')]
            df_28['ID_COD'] = df_28['ENDERECO_ORIG'].astype(str) + " - " + df_28['CODPROD'].astype(str)

            df_aereo = df_aereo.loc[df_aereo['TIPO_PK'] == 'AE']
            df_aereo = df_aereo[['COD_END', 'COD', 'QTDE', 'ENTRADA', 'SAIDA']]
            df_aereo['ID_COD'] = df_aereo['COD_END'].astype(str) + " - " + df_aereo['COD'].astype(str)

            df_baixa = df_28.merge(df_aereo, on= 'ID_COD', how= 'left').fillna(0).drop(columns=['COD_END'])
            df_baixa = df_baixa.merge(df_func, left_on= 'CODFUNCOS', right_on= 'ID_FUNC', how= 'left').drop(columns=['ID_FUNC'])
            df_baixa['TIPO_ABST'] = df_baixa['TIPO_ABST'].fillna("FUNC")  

            col_int = ['QT','QTDE']
            for col in col_int:
                df_baixa = self.ajustar_numero(df_baixa, col, int)

            caso_total = (df_baixa['NIVEL_1'] == 1) & (df_baixa['QT'] >= df_baixa['QTDE'])
            caso_parcial = (df_baixa['NIVEL_1'] == 1) & (df_baixa['QT'] < df_baixa['QTDE'])
            caso_transferencia = (df_baixa['NIVEL_1'] > 1)
            df_baixa['CATEGORIA'] = np.where(
                caso_total
                , "TOTAL"
                ,np.where(
                    caso_parcial
                    ,"PARCIAL"
                    ,np.where(
                        caso_transferencia
                        ,"TRANSFERENCIA"
                        ,"DIVERGENTE"
                    )
                )
            )

            df_pd = df_baixa.loc[(df_baixa['CODFUNCOS'] == 0)].copy()

            nome_ = df_pd['FUNCGER'].str.split()
            df_pd["FUNCGER"] = nome_.str[0] + " " + nome_.str[-1]
            df_pd = df_pd[['NUMOS','CODPROD','DESCRICAO','RUA','PREDIO','APTO', 'RUA_1','PREDIO_1','APTO_1','FUNCGER','CATEGORIA','COD']]
            df_pd = df_pd.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)

            col_fim = ['RECEBIMENTO','EMPILHADOR','ABASTECEDOR']
            df_fim = df_baixa.loc[(~df_baixa['TIPO_ABST'].isin(col_fim)) & (df_baixa['CODFUNCOS'] > 0) & (df_baixa['QTDE'] == 0)]
            nome_ = df_fim['FUNCOSFIM'].str.split()
            df_fim["FUNCOSFIM"] = nome_.str[0] + " " + nome_.str[-1]
            df_fim = df_fim[['NUMOS','CODPROD','DESCRICAO','RUA','PREDIO','APTO','RUA_1','PREDIO_1','APTO_1','FUNCOSFIM','CATEGORIA','COD']]
            df_fim = df_fim.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)
            self.Instancia.stageTime('Transform')
        except Exception as e:
            self.validador.registrar_log(e, "Transform")
            return False
        try:
            self.Instancia.stageTime('Load')
            with pd.ExcelWriter(self.Retorno[0]) as destino:
                df_pd.to_excel(
                    destino
                    ,index= False
                    , sheet_name= 'PEDENTES'
                )
                df_fim.to_excel(
                    destino
                    ,index= False
                    ,sheet_name= 'FIM_MESA'
                )
            self.Instancia.stageTime('Load')
            self.Instancia.conversor(Modulo= "OScheck")
            return True
        except Exception as e:
            self.validador.registrar_log(e, "Load")
            return False
    def carregamento(self, validar):
        lista_de_logs = []
        try:
            if not validar:
                return
            for contador, path in enumerate(self.list_path, 1):
                data_file = os.path.getmtime(path)
                nome_file = os.path.basename(path)

                data_modificacao = dt.datetime.fromtimestamp(data_file)
                data_formatada = data_modificacao.strftime('%d/%m/%Y')
                horas_formatada = data_modificacao.strftime('%H:%M:%S')

                dic_log = {
                    "CONTADOR" : contador
                    ,"ARQUIVO" : nome_file
                    ,"DATA" : data_formatada
                    ,"HORAS" : horas_formatada
                }
                lista_de_logs.append(dic_log)
            dic_retorno = []
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validador.registrar_log(e, "CARREGAMENTO")
            return False
    def outputLog(self, validar):
        ListaOutPut = []
        try:
            if not validar:
                return
            for path in self.Retorno:
                data_file = os.path.getmtime(path)
                nome_file = os.path.basename(path)

                data_modificacao = dt.datetime.fromtimestamp(data_file)
                data_formatada = data_modificacao.strftime('%d/%m/%Y')
                horas_formatada = data_modificacao.strftime('%H:%M:%S')

                Dicionario = {
                    "ARQUIVO": nome_file,
                    "DATA": data_formatada,
                    "HORA": horas_formatada
                }
                ListaOutPut.append(Dicionario)
            return ListaOutPut
        except Exception as e:
            self.validador.registrar_log(e, "output")
            return False
