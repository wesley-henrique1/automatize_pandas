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
        self.list_path = [Wms.geral_1707, Relatorios.rel_96]
        self.VIRTUAIS = [60, 70, 80, 100, 106, 44, 47, 40]
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
            indices = [0, 1, 2, 3, 4, 6, 7, 9, 10, 11, 12, 16, 17, 18, 19, 20]
            base_geral = pd.read_csv(
                self.list_path[0]
                ,header= None
                ,usecols= indices
                ,names= [ColNames.END_GERAL[i] for i in indices]
                ,sep=','
            )   
            R_8596 = pd.read_excel(
                self.list_path[1]
                ,usecols= ['CODPROD',"RUA", 'ALTURAARM', 'QTUNITCX', "QTTOTPAL"]
            )
            end_parado = pd.read_excel(Outros.ou_end, sheet_name= 'AE', usecols= ['COD_END','TIPO'])

            pass
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False

        try:
            aereos_1707 = base_geral.loc[(base_geral['TIPO_END'] == "AE") & (~base_geral['RUA'].isin(self.VIRTUAIS))].copy()
            R_8596 = R_8596.loc[~R_8596['RUA'].isin(self.VIRTUAIS)]
            prod = R_8596.rename(columns= {
                "RUA": "RUA_AP"
            })
            end_parado = end_parado.rename(columns= {"TIPO":'PL_END'})

            prod['QTUNITCX'] = pd.to_numeric(prod['QTUNITCX'], errors= 'coerce').fillna(0).astype(int) 
            prod['QTTOTPAL'] = pd.to_numeric(prod['QTTOTPAL'], errors= 'coerce').fillna(0).astype(int) 
            prod['PL_UN'] = prod['QTTOTPAL'] * prod['QTUNITCX']

            df_completo = aereos_1707.merge(prod, on='CODPROD', how= 'left')
            df_completo = df_completo.merge(end_parado, on= 'COD_END', how= 'left')
            df_completo = df_completo.merge(self.DataFrame, on= 'PL_END', how= 'left')

            cod_int = ['QTDE','DISP_']
            for ws in cod_int:
                df_completo[ws] = pd.to_numeric(df_completo[ws])

            df_completo['PL_ALT'] = df_completo['CAMADA'] * df_completo['ALTURAARM']
            df_completo['DISP_CX'] = df_completo['DISP_'] / df_completo['QTUNITCX']
            df_completo['CAMADA_AE'] = np.ceil(df_completo['DISP_CX'] / df_completo['LASTRO'])
            df_completo['DISP_ALT'] = df_completo['CAMADA_AE'] * df_completo['ALTURAARM']
            df_completo['STATUS'] = np.where(
                df_completo['CODPROD'] > 0
                ,"OCUPADO"
                ,"VAZIO"
            )
                    
            CAT_cond = [
                df_completo['CODPROD'] == 0
                ,df_completo['DISP_ALT'] <= 80
                ,df_completo['DISP_ALT'] <= 135
                ,df_completo['DISP_ALT'] <= 190
                ,df_completo['DISP_ALT'] <= 255
                ,df_completo['DISP_ALT'] > 255
            ]
            CAT_result = [
                "VAZIO"
                ,"PONTA"
                ,"MEDIO"
                ,"INTEIRO"
                ,"INT_255"
                ,"ACIMA_VALIDAR"
            ]
            df_completo['CATEGORIA'] = np.select(CAT_cond, CAT_result, default= '--')

            val = (
                (df_completo['CATEGORIA'].isin(['PONTA', 'MEDIO'])) 
                & (df_completo['CM'] > 135)
            )
            DIV_cond = (
                (df_completo['DISP_'] < df_completo['PL_UN']) 
                & val
            )
            df_completo['DIVERGENCIA'] = np.where(DIV_cond, "VERIFICAR", "CORRETO")

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
                'CODPROD'
                ,'DESCRICAO'
                ,'RUA_AP'
                ,'COD_END'
                ,'RUA'
                ,'PREDIO'
                ,'NIVEL'
                ,'APTO'
                ,'PL_END'
                ,'STATUS'
                ,'CLASSE_AE'
            ]
            etapa_2 = [
                'QTDE'
                ,'ENTRADA'
                ,'SAIDA'
                ,'DISP_'
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
            print("feito")
            return True
        except Exception as e:
            self.validar_erro(e, "Laod")
            return False
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

                print(lista_de_logs)
                print(dic_retorno)
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False


if __name__ ==  "__main__":
    Mapa_Estoque()