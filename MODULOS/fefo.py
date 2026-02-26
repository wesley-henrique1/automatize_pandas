from modulos._settings import Wms, Relatorios, ColNames
import datetime as dt
import pandas as pd
import numpy as np
import os
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

import time
"""
    PEDENCIAS:
    >> criação do Fefo_curva
    >> criação do Fefo_WMS

"""


class auxiliar:
    def mark_step(self,list_time, step_name):
        
        list_time.append((step_name, time.perf_counter()))
        print(f"✔️ Passo '{step_name}' concluído.")

        pass
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
        pass
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
            self.validar_erro(e, "Extract")
            return False


class Fefo_ABST(auxiliar):
    def __init__(self):
        caminho = r"z:\1 - CD Dia\4 - Equipe PCL\6.6 - Recuperação e Indenizado\6.6.3 - FEFO Validade\Curva A-B-C-D\Auditoria"
        nome = "Fefo_Python"
        extensao = ".xlsx"
        self.times = []
        self.fefo_path = os.path.join(caminho, nome + extensao)

        self.list_path = [Wms.wms_07_end, Relatorios.rel_28, Relatorios.rel_96]
        self.list_int = ['2-INTEIRO(1,90)', '1-INTEIRO (2,55)']
        self.list_div = ['6-PRATELEIRA','5-TERCO (0,46)','4-TERCO (0,56)']
        self.list_meio = ['3-MEDIO (0,80)', '7-MEIO PALETE']
        self.virtual = [60,70,80,100,102,106]

        self.pipeline()
        pass
    
    def pipeline(self):
        try:
            start_global = time.perf_counter()
            self.mark_step(self.times,"Extract")
            col_8628 = ['DATA', "CODPROD", "DESCRICAO","NIVEL","ENDERECO_DEST", "RUA_1", "PREDIO_1", "NIVEL_1", "APTO_1","POSICAO"]
            col_8596 = ["CODPROD", "PRAZOVAL","RUA","PREDIO","APTO","PK_END"]
            indices_desejados = [0, 5, 8, 13]
            nomes_colunas = [ColNames.col_end[i] for i in indices_desejados]

            END_1707 = pd.read_csv(
                self.list_path[0]
                ,header= None
                ,names= nomes_colunas
                ,usecols= indices_desejados
            )
            baixa = pd.read_excel(self.list_path[1], usecols= col_8628)
            prod_dados = pd.read_excel(self.list_path[2], usecols= col_8596)

            pass
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        try:
            self.mark_step(self.times,"Transform")
            END_1707['DT_VALIDADE'] = pd.to_datetime(END_1707['DT_VALIDADE'], errors= "coerce")

            grupo = prod_dados['CODPROD'].loc[(prod_dados["PRAZOVAL"] > 0) & (~prod_dados['RUA'].isin(self.virtual))]
            DF_PROD = self.curva_ABC(prod_dados)

            baixa['DATA'] = pd.to_datetime(baixa['DATA'], errors= 'coerce')
            maior_dt = baixa['DATA'].max()
            filtro_dt = maior_dt - dt.timedelta(days= 6)
            df_baixa = baixa.loc[
                (baixa['CODPROD'].isin(grupo))
                & (baixa['NIVEL'].between(2,8))
                & (baixa['NIVEL_1'] == 1)
                & (baixa['DATA'] >= filtro_dt)
                & (baixa['POSICAO'] == 'P')
            ]
            
            df_baixa = df_baixa.merge(END_1707, left_on="ENDERECO_DEST", right_on= "COD_END", how= "left")
            df_completo = df_baixa.merge(DF_PROD, on= "CODPROD", how= "left")
            pass
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            self.mark_step(self.times,"Laod")
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
            df_final.to_excel(self.fefo_path, index= False,sheet_name= "FEFO")
            end_global = time.perf_counter()

            for i in range(1, len(self.times)):
                diff = self.times[i][1] - self.times[i-1][1]
                print(f"{self.times[i][0]}: {diff:.0f} seg")

            print(f"\n>>>>Tempo Total: {end_global - start_global:.0f} seg<<<")
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


"""
    AREA DE TESTE
    clases 
"""
if __name__ == "__main__":
    Fefo_ABST()