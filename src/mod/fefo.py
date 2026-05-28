from ..lib.settings import Wms, Relatorios, Gestao, Filial_18, ColNames, OutPut
from ..lib import ValidarErros, MonitorETL

import datetime as dt
import pandas as pd
import numpy as np
import os

class __auxiliares:
    def curva_ABC(self, dataframe):
        try:
            TEMPORARIO_DF = dataframe.loc[(~dataframe["RUA"].isin(self.virtual)) & (dataframe["PRAZOVAL"] > 0)].copy()

            TEMPORARIO_DF['CONCAT'] = TEMPORARIO_DF["RUA"].astype(str) + " - " + TEMPORARIO_DF["PREDIO"].astype(str)
            filtro = (TEMPORARIO_DF["APTO"].between(100, 199)) & (TEMPORARIO_DF["PK_END"].isin(self.list_meio))

            TEMPORARIO_DF.loc[filtro, 'qt_meio'] = TEMPORARIO_DF[filtro].groupby('CONCAT')['CONCAT'].transform('count')
            TEMPORARIO_DF['FREQ_PROD'] = TEMPORARIO_DF['CONCAT'].map(TEMPORARIO_DF['CONCAT'].value_counts())

            INT = (TEMPORARIO_DF['FREQ_PROD'] <= 2) & (TEMPORARIO_DF["PK_END"].isin(self.list_int))
            MEIO =(TEMPORARIO_DF['qt_meio'] <= 2) & (TEMPORARIO_DF["PK_END"].isin(self.list_meio))
            DIV = (TEMPORARIO_DF['FREQ_PROD'] > 2)
            
            curva_A = (TEMPORARIO_DF["PRAZOVAL"].between(30,180))
            curva_B = (TEMPORARIO_DF["PRAZOVAL"].between(181,365))
            curva_C = (TEMPORARIO_DF["PRAZOVAL"] >= 366)

            cond_status = [INT, MEIO, DIV]
            cond_curva= [curva_A, curva_B, curva_C]

            escolha_STATUS = ["INT","MEIO", "DIV"]
            escolha_curva = ["Curva_A", "Curva_B","Curva_C"]

            TEMPORARIO_DF['TIPO_END'] = np.select(
                cond_status
                ,escolha_STATUS
                ,default="VAL"
            )
            TEMPORARIO_DF['CURVA_CD'] = np.select(
                cond_curva
                ,escolha_curva
                ,default= "Sem Risco"
            )
            TEMPORARIO_DF = TEMPORARIO_DF.drop(columns= ["qt_meio", "CONCAT", "FREQ_PROD"])
            return TEMPORARIO_DF
        except Exception as e:
            self.validador.registrar_log(e, "AUX_Extract")
            return False

    pass
class FefoAbst(__auxiliares):
    validador = ValidarErros(fonte="Fefo Abastecimento")
    def __init__(self):
        self.list_path = [Wms.endereco07, Relatorios._8628, Relatorios._8596]
        self.Retorno = [OutPut.Fefo8628]

        self.list_int = ['2-INTEIRO(1,90)', '1-INTEIRO (2,55)']
        self.list_div = ['6-PRATELEIRA','5-TERCO (0,46)','4-TERCO (0,56)']
        self.list_meio = ['3-MEDIO (0,80)', '7-MEIO PALETE']
        self.virtual = [60,70,80,100,102,106]

        self.Instancia = MonitorETL()
        pass
    
    def pipeline(self):
        try:
            self.Instancia.stageTime('Extract')
            col_8628 = ['DATA', "CODPROD", "DESCRICAO","NIVEL","ENDERECO_DEST", "RUA_1", "PREDIO_1", "NIVEL_1", "APTO_1","POSICAO"]
            col_8596 = ["CODPROD", "PRAZOVAL","RUA","PREDIO","APTO","PK_END"]
            indices_desejados = [0, 5, 8, 13]
            nomes_colunas = [ColNames.Endereco[i] for i in indices_desejados]

            END_1707 = pd.read_csv(
                self.list_path[0]
                ,header= None
                ,names= nomes_colunas
                ,usecols= indices_desejados
            )
            baixa = pd.read_excel(self.list_path[1], usecols= col_8628)
            prod_dados = pd.read_excel(self.list_path[2], usecols= col_8596)

            self.Instancia.stageTime('Extract')
        except Exception as e:
            self.validador.registrar_log(e, "ABST_Extract")
            return False
        try:
            self.Instancia.stageTime('Transform')
            END_1707['DT_VALIDADE'] = pd.to_datetime(END_1707['DT_VALIDADE'], dayfirst=True, errors= "coerce")

            grupo = prod_dados['CODPROD'].loc[(prod_dados["PRAZOVAL"] > 0) & (~prod_dados['RUA'].isin(self.virtual))]
            DF_PROD = self.curva_ABC(prod_dados)

            baixa['DATA'] = pd.to_datetime(baixa['DATA'], dayfirst=True, errors= 'coerce')
            maior_dt = baixa['DATA'].max()
            filtro_dt = maior_dt - dt.timedelta(days= 6)
            df_baixa = baixa.loc[
                (baixa['CODPROD'].isin(grupo))
                & (baixa['NIVEL'].between(2,8))
                & (baixa['NIVEL_1'] == 1)
                & (baixa['DATA'] >= filtro_dt)
                & (baixa['POSICAO'] == 'C')
            ]
            
            df_baixa = df_baixa.merge(END_1707, left_on="ENDERECO_DEST", right_on= "COD_END", how= "left")
            df_completo = df_baixa.merge(DF_PROD, on= "CODPROD", how= "left")
            self.Instancia.stageTime('Transform')
        except Exception as e:
            self.validador.registrar_log(e, "ABST_Transform")
            return False
        try:
            self.Instancia.stageTime('Load')
            COLUNAS_FEFO = [
                "DATA"
                , "CODPROD"
                , "DESCRICAO"
                , "NIVEL"
                , "RUA_1"
                , "PREDIO_1"
                , "NIVEL_1"
                , "APTO_1"
                , "POSICAO"
                , "PK_END"
                , "PRAZOVAL"
                , "TIPO_END"
                , "CURVA_CD"
                , "DT_VALIDADE"
            ]

            df_final = df_completo[COLUNAS_FEFO]
            df_final.to_excel(self.Retorno[0], index= False,sheet_name= "FEFO")
            self.Instancia.stageTime('Load')
            self.Instancia.conversor(Modulo= "Fefo abst")
            return True
        except Exception as e:
            self.validador.registrar_log(e, "ABST_Load")
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
class FefoCurva(__auxiliares):
    validador = ValidarErros(fonte="Fefo Curva")
    def __init__(self):
        self.list_path = [Relatorios._8668, Gestao._286, Filial_18._286, Relatorios._8596, Wms.endereco07]
        self.Retorno = [OutPut.Fefo8668]

        self.list_int = ['2-INTEIRO(1,90)', '1-INTEIRO (2,55)']
        self.list_div = ['6-PRATELEIRA','5-TERCO (0,46)','4-TERCO (0,56)']
        self.list_meio = ['3-MEDIO (0,80)', '7-MEIO PALETE']
        self.virtual = [60,70,80,100,102,106]

        self.list_time = []
        self.dt_hoje = dt.datetime.now()

        self.Instancia = MonitorETL()
        pass
    
    def pipeline(self):
        try:
            self.Instancia.stageTime('Extract')
            col_est = ["Código", "Giro dia", "Est. dia"]
            col_prod = ["CODPROD","RUA","PREDIO","APTO","PK_END", "DTULTENT","PRAZOVAL", "FORNECEDOR"]
            indices_desejados = [0, 5, 8, 13]
            col_07 = [ColNames.Endereco[i] for i in indices_desejados]

            base_8668 = pd.read_excel(self.list_path[0])
            est_PROD = pd.read_excel(self.list_path[1], usecols= col_est)
            est_F18 = pd.read_excel(self.list_path[2], usecols= col_est)
            dados_PROD = pd.read_excel(self.list_path[3], usecols= col_prod)
            end_1707 = pd.read_csv(self.list_path[4], header= None, usecols= indices_desejados, names= col_07)
            self.Instancia.stageTime('Extract')
            pass
        except Exception as e:  
            self.validador.registrar_log(e, "curva-Extract")
            return False
        try:
            self.Instancia.stageTime('Transform')
            cod_fefo = dados_PROD["CODPROD"].loc[dados_PROD["PRAZOVAL"] > 0]

            """>> Tratamento da base 8668"""
            df_base = base_8668.loc[(base_8668['RUA'] > 0) & (base_8668['NIVEL'].between(2,8)) &(base_8668['CODPROD'].isin(cod_fefo))].copy()

            """>> Tratamento da auxiliar 286"""
            F11 = est_PROD.rename(columns= {
                "Código" : "CODPROD"
                ,"Est. dia": "EST_DIA"
                ,"Giro dia": "GIRO_DIA"
            })
            F18 = est_F18.rename(columns= {
                "Código" : "CODPROD"
                ,"Est. dia": "EST_DIA_18"
                ,"Giro dia": "GIRO_DIA_18"
            })
            df_estoque = F11.merge(F18, on="CODPROD", how="outer").fillna(0)
            df_estoque["EST_DIA"] = round(df_estoque["EST_DIA"] + df_estoque["EST_DIA_18"],2)
            df_estoque["GIRO_DIA"] = round(df_estoque["GIRO_DIA"] + df_estoque["GIRO_DIA_18"],2 )
            df_estoque = df_estoque.drop(columns= ["EST_DIA_18", "GIRO_DIA_18"])

            """>> Tratamento da auxiliar 8596"""
            dados_PROD = self.curva_ABC(dados_PROD)
            dados_PROD['DTULTENT'] = pd.to_datetime(dados_PROD['DTULTENT'], dayfirst=True, errors= "coerce")
            dados_PROD['PRAZOVAL'] = pd.to_timedelta(dados_PROD['PRAZOVAL'], unit= 'D', errors= 'coerce')
            dados_PROD = dados_PROD.drop(columns= ["RUA","PREDIO", "APTO", "PK_END"])

            """>> Tratamento da auxiliar 1707"""
            end_AP = end_1707.loc[end_1707['TIPO_PK'] == "AP"].copy()
            end_AP = end_AP.rename(columns={
                'COD_END': "COD_AP"
                ,'DT_VALIDADE':"VALIDADE_AP"
                })
            end_AP['VALIDADE_AP'] = pd.to_datetime(end_AP['VALIDADE_AP'], dayfirst=True , errors= 'coerce')
            end_1707['DT_VALIDADE'] = pd.to_datetime(end_1707['DT_VALIDADE'], dayfirst=True, errors= 'coerce')

            df_completo = df_base.merge(df_estoque, on= "CODPROD", how= "left")
            df_completo = df_completo.merge(dados_PROD, on= "CODPROD", how= "left")

            df_completo = df_completo.merge(end_1707, left_on= "PULMAO", right_on= "COD_END", how= 'left').drop(columns= ["COD_END","TIPO_PK", "COD"])
            df_completo = df_completo.merge(end_AP, left_on= "PICKING", right_on= "COD_AP", how= 'left').drop(columns= ["COD_AP","TIPO_PK", "COD"])
            try:
                df_completo['PREVISAO_VL'] = df_completo['DTULTENT'] + df_completo['PRAZOVAL']
                df_completo['PREVI_DIF'] = (df_completo['PREVISAO_VL'] - df_completo['DT_VALIDADE']).dt.days
                df_completo['DIAS_PARADO'] =  (self.dt_hoje - df_completo['DTENTRADA']).dt.days

                df_completo['VL_SHELF'] = df_completo['DT_VALIDADE'] + df_completo['PRAZOVAL']
                df_completo['SHELF_DIF'] = (df_completo['VL_SHELF'] - df_completo['DT_VALIDADE']).dt.days
                df_completo['AP_VS_AE'] = np.where(
                    df_completo['VALIDADE_AP'] > df_completo['DT_VALIDADE']
                    ,"ERRO"
                    ,"CORRETO"
                )

                df_completo['prazo'] = (df_completo['DT_VALIDADE'] - self.dt_hoje).dt.days
                end_A = (df_completo['prazo'].between(30,180))
                end_B = (df_completo['prazo'].between(181,365))
                end_C = (df_completo['prazo'] >= 366)
                cond_end= [end_A, end_B, end_C]
                escolha_curva = ["Curva_A", "Curva_B","Curva_C"]

                df_completo['CURVA_END'] = np.select(cond_end, escolha_curva, default= "-")
                df_completo['FILTRO_30D']  = df_completo['DT_VALIDADE'] -  df_completo['VALIDADE_AP']

                pass
            except Exception as e:
                self.validador.registrar_log(e, "curva-T-calculada")
                return False
            self.Instancia.stageTime('Transform')
        except Exception as e:
            self.validador.registrar_log(e, "curva-Transform")
            return False
        try:
            self.Instancia.stageTime('Load')
            col_int = ["RUA", "PREDIO", "NIVEL", "APTO", "PREVI_DIF"]
            for col in  col_int:
                df_completo[col] = pd.to_numeric(df_completo[col], errors= 'coerce').fillna(0).astype(int)

            col_str = df_completo.select_dtypes(include=['object', 'string']).columns.tolist()
            for col in col_str:
                df_completo[col] = df_completo[col].fillna("-")
                
            col_base = [
                "CODPROD"
                ,"DESCRICAO"
                ,"PULMAO"
                ,"RUA"
                ,"PREDIO"
                ,"NIVEL"
                ,"APTO"
                ,"PICKING"
                ,"RUA_AP"
                ,"PREDIO_AP"
                ,"NIVEL_AP"
                ,"APTO_AP"
                ,"DTENTRADA"
                ,"DTULTENT"
                ,'TIPO_END'
            ]
            col_calculadas = [
                "PREVISAO_VL"
                ,"PREVI_DIF"
                ,"DIAS_PARADO"
                ,"AP_VS_AE"
                ,"VL_SHELF"
                ,"SHELF_DIF"
                ,"CURVA_END"
                ,"CURVA_CD"
            ]
            col_final = [
                'VALIDADE_AP'
                ,'DT_VALIDADE'
                ,"PRAZOVAL"
                ,'EST_DIA'
                , 'GIRO_DIA'
                , "FORNECEDOR"
                , "FILTRO_30D"
            ]

            df_completo = df_completo[col_base + col_calculadas + col_final]
            df_completo.to_excel(self.Retorno[0], index= False, sheet_name= "Fefo")
            self.Instancia.stageTime('Load')
            self.Instancia.conversor(Modulo= "Fefo Curvas")
            return True
        except Exception as e:
            self.validador.registrar_log(e, "curva-Load")
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
