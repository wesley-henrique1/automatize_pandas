from modulos._settings import Wms, Relatorios, ColNames
import datetime as dt
import pandas as pd
import numpy as np
import os
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
"""
    PEDENCIAS:
    >> criação do Fefo_curva
    >> criação do Fefo_WMS

"""


class auxiliar:
    def validar_erro(self, e, etapa):
        largura = 64
        mapeamento = {
            PermissionError: "Arquivo aberto ou sem permissão. Feche o Excel.",
            FileNotFoundError: "Arquivo de origem não encontrado. Verifique a pasta 'base_dados'.",
            KeyError: f"Coluna ou chave não encontrada: {e}",
            TypeError: f"Incompatibilidade de tipo: {e}",
            ValueError: f"Formato de dado inválido: {e}",
            NameError: f"Variável ou função não definida: {e}"
        }
        
        msg = mapeamento.get(type(e), f"Erro não mapeado: {e}")
        agora = dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        log_conteudo = (
            f"{'='* largura}\n"
            f"FONTE: FEFO.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")

class Fefo_ABST(auxiliar):
    def __init__(self):
        caminho = r"z:\1 - CD Dia\4 - Equipe PCL\6.6 - Recuperação e Indenizado\6.6.3 - FEFO Validade\Curva A-B-C-D\Auditoria"
        nome = "Fefo_Python"
        extensao = ".xlsx"
        self.fefo_path = os.path.join(caminho, nome + extensao)

        self.list_path = [Wms.wms_07_end, Relatorios.rel_28, Relatorios.rel_96]
        self.list_int = ['2-INTEIRO(1,90)', '1-INTEIRO (2,55)']
        self.list_div = ['6-PRATELEIRA','5-TERCO (0,46)','4-TERCO (0,56)']
        self.list_meio = ['3-MEDIO (0,80)', '7-MEIO PALETE']
        self.virtual = [60,70,80,100,102,106]

        pass
    
    def pipeline(self):
        try:
            col_8628 = ['DATA', "CODPROD", "DESCRICAO","NIVEL", "RUA_1", "PREDIO_1", "NIVEL_1", "APTO_1","POSICAO"]
            col_8596 = ["CODPROD", "PRAZOVAL","RUA","PREDIO","APTO","PK_END"]

            end_1707 = pd.read_csv(self.list_path[0], header= None, names= ColNames.col_end)
            baixa = pd.read_excel(self.list_path[1], usecols= col_8628)
            produto = pd.read_excel(self.list_path[2], usecols= col_8596)

            pass
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        try:
            """Tratamento rotina 8596"""
            df_PRODUTO = produto.loc[~produto['RUA'].isin(self.virtual)].copy()
            df_PRODUTO['CONCAT'] = df_PRODUTO['RUA'].astype(str) + " - " + df_PRODUTO['PREDIO'].astype(str)
            filtro = (df_PRODUTO['APTO'].between(100, 199)) & (df_PRODUTO['PK_END'].isin(self.list_meio))

            df_PRODUTO.loc[filtro, 'qt_meio'] = df_PRODUTO[filtro].groupby('CONCAT')['CONCAT'].transform('count')
            df_PRODUTO['FREQ_PROD'] = df_PRODUTO['CONCAT'].map(df_PRODUTO['CONCAT'].value_counts())

            INT = (df_PRODUTO['FREQ_PROD'] <= 2) & (df_PRODUTO['PK_END'].isin(self.list_int))
            MEIO =(df_PRODUTO['qt_meio'] <= 2) & (df_PRODUTO['PK_END'].isin(self.list_meio))
            DIV = (df_PRODUTO['FREQ_PROD'] > 2)
            
            curva_A = (df_PRODUTO['PRAZOVAL'].between(30,180))
            curva_B = (df_PRODUTO['PRAZOVAL'].between(181,365))
            curva_C = (df_PRODUTO['PRAZOVAL'] >= 366)

            cond_status = [INT, MEIO, DIV]
            cond_curva= [curva_A, curva_B, curva_C]

            escolha_STATUS = ["INT","MEIO", "DIV"]
            escolha_curva = ["Curva_A", "Curva_B","Curva_C"]

            df_PRODUTO['TIPO_END'] = np.select(
                cond_status
                ,escolha_STATUS
                ,default="VAL"
            )
            df_PRODUTO['CURVA_CD'] = np.select(
                cond_curva
                ,escolha_curva
                ,default= "Sem Risco"
            )

            """Tratamento rotina 1707"""
            df_aereos = end_1707[["COD", "DT_VALIDADE"]].copy()
            df_aereos['DT_VALIDADE'] = pd.to_datetime(
                df_aereos['DT_VALIDADE'], 
                errors='coerce'
            ).fillna(pd.to_datetime("2002-09-05"))

            """"""
            dt_maior = baixa['DATA'].max()
            penultimo = dt_maior - dt.timedelta(days= 7)
            df_baixa = baixa.loc[(baixa['DATA'] > penultimo) &(baixa['NIVEL'].between(2,8)) & (baixa['NIVEL_1'] == 1)].copy()

            base_completa = df_baixa.merge(df_PRODUTO, on= "CODPROD", how= "left")
            base_completa = base_completa.merge(df_aereos, left_on= "CODPROD", right_on= "COD", how= "left").drop(columns= "COD")
            base_completa = base_completa.drop(columns= ["qt_meio", "CONCAT", "FREQ_PROD"])

            pass
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            base_completa.to_excel(self.fefo_path, sheet_name= "BASE_FEFO", index= False)

            return True
        except Exception as e:
            self.validar_erro(e, "Laod")
            return False
    def carregamento(self):
        lista_de_logs = []
        try:
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
            print("ok")
            dic_retorno = []
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False
class Fefo_curva(auxiliar):
    def __init__(self):
        self.list_path = []
        pass
    
    def pipeline(self):
        try:
            pass
        except Exception as e:  
            self.validar_erro(e, "Extract")
            return False
        try:
            pass
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            pass
        except Exception as e:
            self.validar_erro(e, "Laod")
            return False
    def carregamento(self):
        lista_de_logs = []
        try:
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
            print("ok")
            dic_retorno = []
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False
class Fefo_WMS(auxiliar):
    def __init__(self):
        self.list_path = []
        pass
    
    def pipeline(self):
        try:
            pass
        except Exception as e:  
            self.validar_erro(e, "Extract")
            return False
        try:
            pass
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            pass
        except Exception as e:
            self.validar_erro(e, "Laod")
            return False
    def carregamento(self):
        lista_de_logs = []
        try:
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
            print("ok")
            dic_retorno = []
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False
