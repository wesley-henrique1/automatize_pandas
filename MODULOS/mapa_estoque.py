from _settings import Output, Relatorios, ColNames,Wms, Outros
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

import datetime as dt
import pandas as pd
import numpy as np
import os

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
            f"FONTE: Mapa_Estoque.py | ETAPA: {etapa} | DATA: {agora}\n"
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
    def categorizar_AE(self, Rua_AE, Rua_AP, map_dic):
        try:
            r_ae = int(Rua_AE)
            r_ap = int(Rua_AP)
        except:
            return "--"

        if r_ae == r_ap:
            return "DT"
        excecoes = map_dic.get(r_ap, [])
        
        if (r_ae in excecoes) or (abs(r_ae - r_ap) == 1):
            return "VZ"
        
        return "FR"
    pass
class Mapa_Estoque(auxiliar):
    def __init__(self):
        self.list_path = [Wms.wms_07_end, Relatorios.rel_96, Outros.ou_end]

        self.estruturas = {
            "INTEIRO (2,55)": [255, "INT_255"]
            ,"INTEIRO(1,90)": [190, "INTEIRO"]
            ,"INTEIRO(1,35)": [135, "MEDIO"]
            ,"MEDIO (0,80)": [80, "PONTA"]
            ,"TERCO (0,56)": [56, "PONTA"]
            ,"TERCO (0,46)": [46, "PONTA"]
        }
        self.DataFrame = pd.DataFrame({
            "PL_END": ["INTEIRO (2,55)","INTEIRO(1,90)","INTEIRO(1,35)","MEDIO (0,80)","TERCO (0,56)","TERCO (0,46)"]
            ,"CM": [255, 190, 135, 80, 56, 46]
            ,"CLASSE_AE": ["INT_255", "INTEIRO", "MEDIO","PONTA","PONTA", "PONTA"]
        })
        self.map_ruas = {
            13: [12]
            ,14: [15,44]
            ,30: [28,29]
            ,31: [13,14,15,16,17,18,19,32,44]
            ,32: [13,14,15,16,17,18,19,31,44]
            ,33: [34,35,36,37,38,39]
            ,34: [33,35,36,37,38,39]
            ,35: [33,34,36,37,38,39]
            ,36: [33,34,35,37,38,39]
            ,37: [33,34,35,36,38,39]
            ,38: [33,34,35,36,37,39]
            ,39: [33,34,35,36,37,38]
        }

        self.pipeline()
        pass

    def pipeline(self):
        try:
            R_1707 = pd.read_csv(
                self.list_path[0]
                ,header= None
                ,decimal= ','
                ,dtype= {15: float, 18: float}
                ,names= ColNames.col_end
            )
            R_8596 = pd.read_excel(
                self.list_path[1]
                ,usecols= ['CODPROD',"RUA", 'ALTURAARM', 'QTUNITCX', "QTTOTPAL"]
            )
            END = pd.read_excel(
                self.list_path[2]
                , sheet_name= 'AE'
                , usecols= ['COD_END', 'RUA']
            )

            pass
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False

        try:
            VIRTUAIS = [60, 70, 80, 100, 106, 44, 47, 40]
            filtro = END.loc[~END['RUA'].isin(VIRTUAIS)]


            df_AE = R_1707.loc[(R_1707['TIPO_PK'] == 'AE') & R_1707['COD_END'].isin(filtro['COD_END'])]
            df_AE = df_AE[['COD_END', 'PREDIO', 'NIVEL', 'APTO', 'STATUS', 'COD', 'DESC', 'LASTRO', 'CAMADA','TIPO_PK', 'PL_END', 'QTDE', 'ENTRADA', 'SAIDA', 'DISP']]

            R_8596 = R_8596.loc[R_8596['RUA'].isin(filtro['RUA'])]
            prod = R_8596.rename(columns= {
                "RUA": "RUA_AP"
            })
            print('passou aqui')
            prod['QTUNITCX'] = pd.to_numeric(prod['QTUNITCX'], errors= 'coerce').fillna(0).astype(int) 
            prod['QTTOTPAL'] = pd.to_numeric(prod['QTTOTPAL'], errors= 'coerce').fillna(0).astype(int) 
            prod['PL_UN'] = prod['QTTOTPAL'] * prod['QTUNITCX']

            df_completo = df_AE.merge(prod, left_on= "COD", right_on= 'CODPROD', how= 'left').drop(columns= "CODPROD")
            df_completo = df_completo.merge(self.DataFrame, on= 'PL_END', how= 'left')
            df_completo = df_completo.merge(END, on= 'COD_END', how= 'left')
            print('passou aqui tambem')

            df_completo['PL_ALT'] = df_completo['CAMADA'] * df_completo['ALTURAARM']
            df_completo['DISP_CX'] = df_completo['DISP'] / df_completo['QTUNITCX']
            df_completo['CAMADA_AE'] = np.ceil(df_completo['DISP_CX'] / df_completo['LASTRO'])
            df_completo['DISP_ALT'] = df_completo['CAMADA_AE'] * df_completo['ALTURAARM']
            
            CAT_cond = [
                df_completo['DISP_ALT'] <= 80
                ,df_completo['DISP_ALT'] <= 135
                ,df_completo['DISP_ALT'] <= 190
                ,df_completo['DISP_ALT'] <= 255
            ]
            CAT_result = [
                "PONTA"
                ,"MEDIO"
                ,"INTEIRO"
                ,"INT_255"
            ]
            df_completo['CATEGORIA'] = np.select(CAT_cond, CAT_result, default= '--')
            print('passou aqui antes da gambiarra')
            val = (
                (df_completo['CATEGORIA'].isin(['PONTA', 'MEDIO'])) 
                & (df_completo['CM'] > 135)
            )
            DIV_cond = (
                (df_completo['DISP'] < df_completo['PL_UN']) 
                & val
            )
            df_completo['DIVERGENCIA'] = np.where(DIV_cond, "VERIFICAR", "CORRETO")
            print('passou aqui depois da gambiarra')
            col_int = ['RUA', 'RUA_AP']
            for col in col_int:
                df_completo[col] = pd.to_numeric(df_completo[col]).fillna(0)
            df_completo['LOC_AEREO'] = df_completo.apply(
                lambda x: self.categorizar_AE(x['RUA'], x['RUA_AP'], self.map_ruas), 
                axis= 1
            )

            pass
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False

        try:
            etapa_1 = [
                'COD'
                ,'DESC'
                ,'RUA_AP'
                ,'COD_END'
                ,'RUA'
                ,'PREDIO'
                ,'NIVEL'
                ,'APTO'
                ,'PL_END'
                ,'CLASSE_AE'
            ]
            etapa_2 = [
                'QTDE'
                ,'ENTRADA'
                ,'SAIDA'
                ,'DISP'
            ]
            etapas_KPI  = [
                'DISP_CX'
                ,'PL_ALT'
                ,'DISP_ALT'
                ,'CAMADA_AE'
                ,'CATEGORIA'
                ,'LOC_AEREO'
                ,'DIVERGENCIA'
            ]
            df_completo = df_completo[etapa_1 + etapa_2 + etapas_KPI]
            df_completo = df_completo.sort_values(by=["RUA", "PREDIO"], ascending= True)
            df_completo.to_excel(Output.mapa_estoque,index= False, sheet_name="Analise ae")
            return True
            pass
        except Exception as e:
            self.validar_erro(e, "Laod")
            return False

        pass
    def carregamento(self):
        lista_de_logs = []
        dic_retorno = []
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
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False


if __name__ ==  "__main__":
    
    Mapa_Estoque()