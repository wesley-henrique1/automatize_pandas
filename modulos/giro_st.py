from modulos._settings import Relatorios, Gestao, Filial_18, Wms, ColNames, OutPut

import pandas as pd
import numpy as np
import datetime as dt
import warnings
import os
warnings.simplefilter(action='ignore', category=UserWarning)

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
            f"FONTE: giro_st.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")
class Giro_Status(auxiliar):
    def __init__(self):
        self.hoje = dt.datetime.now()
        self.dias = 30
        self.VIRTUAIS = [0,60, 70, 80, 100, 106, 44]

        self.list_path = [
            Relatorios._8596, Gestao._286
            ,Filial_18._8596, Filial_18._286
            ,Wms.endereco07
        ]
        self.saida = OutPut.GiroStatus
        self.pipeline()
        pass
    def pipeline(self):
        try:
            col_8596 = [
                'DTULTENT', 'DTULTSAIDA', 'CODPROD', 'DESCRICAO', 'OBS2', 'QTUNITCX', 'RUA', 'PREDIO', 'NIVEL', 'APTO','QTESTGER'
            ]
            col_286= [
                'Código', 'Estoque', 'Bloqueado(Qt.Bloq.-Qt.Avaria)', 'Qt.Avaria','Reservado', 'Disponível', 'Custo ult. ent.', 'Comprador'
            ]

            col_1707 = [5,13]
            _col_ = ColNames.Endereco

            dados_F11 = pd.read_excel(self.list_path[0], usecols= col_8596)
            aux_286_F11 = pd.read_excel(self.list_path[1], usecols= col_286)
            
            dados_F18 = pd.read_excel(self.list_path[2], usecols= col_8596)
            aux_286_F18 = pd.read_excel(self.list_path[3], usecols= col_286)

            aux_1707 = pd.read_csv(self.list_path[4], header= None, usecols= col_1707, names=[_col_[5], _col_[13]])

            pass
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        try:
            try:
                df_estoque = aux_286_F11.merge(aux_286_F18, on= 'Código', how= 'outer').fillna(0)
                df_estoque = df_estoque.rename(columns={'Código': "CODPROD"})
                col_zero = [
                    'Estoque_x', 'Estoque_y'
                    ,'Bloqueado(Qt.Bloq.-Qt.Avaria)_x', 'Bloqueado(Qt.Bloq.-Qt.Avaria)_y'
                    ,'Qt.Avaria_x', 'Qt.Avaria_y'
                    ,'Custo ult. ent._x', 'Custo ult. ent._y'
                    ,'Reservado_x','Reservado_y'
                ]
                for col in col_zero:
                    df_estoque[col] = pd.to_numeric(df_estoque[col]).fillna(0)

                condicoes = [
                    df_estoque["Custo ult. ent._x"] > df_estoque["Custo ult. ent._y"]
                    ,df_estoque["Custo ult. ent._x"] < df_estoque["Custo ult. ent._y"]
                ]
                resultados = [
                    "F11"
                    ,"F18"
                ]
                res_custo = [
                    round(df_estoque["Custo ult. ent._x"],2)
                    ,round(df_estoque["Custo ult. ent._y"],2)
                ]

                c_x = df_estoque['Comprador_x'].fillna(0)
                c_y = df_estoque['Comprador_y'].fillna(0)

                compr_cond = [
                    (c_x == c_y)
                    ,(c_x == 0) & (c_y != 0)
                    ,(c_x != 0) & (c_y == 0)
                    ,(c_x != c_y)
                ]
                compr_resultados = [
                    df_estoque['Comprador_x']
                    ,df_estoque['Comprador_y']
                    ,df_estoque['Comprador_x']
                    ,df_estoque['Comprador_x']
                ]

                df_estoque["FILIAL"] = np.select(condicoes, resultados, default= "F11-F18")
                df_estoque["CUSTO"] = np.select(
                    condicoes, res_custo
                    ,default= round(df_estoque["Custo ult. ent._x"],2)
                )
                df_estoque['COMPRADOR'] = np.select(compr_cond, compr_resultados, default="VERIFICAR")

                nomes_separados = df_estoque['COMPRADOR'].str.split()

                df_estoque['COMPRADOR'] = np.where(
                    nomes_separados.str.len() > 1
                    ,nomes_separados.str[0] + " " + nomes_separados.str[-1]
                    ,nomes_separados.str[0]        
                )
                df_estoque['CODPROD'] = pd.to_numeric(df_estoque['CODPROD']).astype(int)
                df_estoque["ESTOQUE"] = df_estoque["Estoque_x"] + df_estoque["Estoque_y"]
                df_estoque["BLOQ"] = df_estoque["Bloqueado(Qt.Bloq.-Qt.Avaria)_x"] + df_estoque["Bloqueado(Qt.Bloq.-Qt.Avaria)_y"]
                df_estoque["AVARIA"] = df_estoque["Qt.Avaria_x"] + df_estoque["Qt.Avaria_y"]
                df_estoque['RESERVADO'] = round(df_estoque['Reservado_x'] + df_estoque['Reservado_y'])

                df_estoque["TOTAL_BLOQ"] = df_estoque["BLOQ"] + df_estoque["AVARIA"]
                df_estoque['DISP'] = df_estoque["ESTOQUE"] - df_estoque["TOTAL_BLOQ"]
                df_estoque['CUSTO_EST'] = round(df_estoque["CUSTO"] * df_estoque["DISP"], 2)
                df_estoque['_RESERVA_'] = np.where(
                    df_estoque['RESERVADO'] > 0
                    ,"S"
                    ,"N"
                )
                
                drop_x = ["Qt.Avaria_x","Reservado_x","Disponível_x","Custo ult. ent._x"
                        ,"Comprador_x"
                        ,"Bloqueado(Qt.Bloq.-Qt.Avaria)_x", "Estoque_x"
                ]
                drop_y = ["Qt.Avaria_y","Reservado_y","Disponível_y","Custo ult. ent._y"
                        ,"Comprador_y"
                        ,"Bloqueado(Qt.Bloq.-Qt.Avaria)_y", "Estoque_y"]
                df_estoque = df_estoque.drop(columns= drop_x + drop_y)
                pass
            except Exception as e:
                self.validar_erro(e, "T_286")
                return False

            try:
                virtual_prod = dados_F11.loc[(dados_F11['RUA'].isin(self.VIRTUAIS))]
                dados_F11 = dados_F11.loc[(~dados_F11['RUA'].isin(self.VIRTUAIS))]

                dados_F18 = dados_F18.loc[(dados_F18['QTESTGER'] > 0) & (~dados_F18['CODPROD'].isin(virtual_prod['CODPROD']))].copy()
                dados_F18 = dados_F18[['CODPROD', 'DTULTSAIDA', 'DTULTENT']]

                dados_prod = dados_F11.merge(dados_F18, on= 'CODPROD', how= "outer")

                col_int = ['QTUNITCX','RUA','PREDIO','APTO']
                dt_col = ["DTULTSAIDA_x", "DTULTSAIDA_y", "DTULTENT_x", "DTULTENT_y"]

                for col in col_int:
                    dados_prod[col] = pd.to_numeric(dados_prod[col], errors= 'coerce').fillna(0).astype(int)

                for col in dt_col:
                    dados_prod[col] = pd.to_datetime(dados_prod[col], dayfirst= True, errors= 'coerce')

                dados_prod = dados_prod.loc[~dados_prod['RUA'].isin(self.VIRTUAIS)]

                dados_prod['DT_SAIDA'] = dados_prod[['DTULTSAIDA_x', 'DTULTSAIDA_y']].max(axis=1)
                dados_prod['DT_ENTRADA'] = dados_prod[['DTULTENT_x', 'DTULTENT_y']].max(axis=1)
                dados_prod['OBS2'] = dados_prod['OBS2'].fillna("ATIVO")
                dados_prod['DIAS_S'] = (self.hoje - dados_prod['DT_SAIDA']).dt.days.fillna(0).astype(int)
                dados_prod['DIAS_E'] = (self.hoje - dados_prod['DT_ENTRADA']).dt.days.fillna(0).astype(int)
                dados_prod['PARADO'] = np.where(
                    (dados_prod['DIAS_S'] > 30) & (dados_prod['DIAS_E'] > 30)
                    ,"S"
                    ,"N"
                )
                dados_prod = dados_prod.drop(columns=["DTULTSAIDA_x", "DTULTENT_x", "DTULTSAIDA_y", "DTULTENT_y", "QTESTGER"])

                pass
            except Exception as e:
                self.validar_erro(e, "T_8596")
                return False

            try:
                grupo_AE = aux_1707.groupby("COD").agg(
                    QT_AE = ('TIPO_PK', 'count')
                ).reset_index()
                grupo_AE['QT_AE'] = grupo_AE['QT_AE'].astype(int)

                pass
            except Exception as e:
                self.validar_erro(e, "T_1707")
                return False
            pass

            df_completo = dados_prod.merge(df_estoque, on= 'CODPROD', how= 'left')

            bloq_quest = [
                (df_completo['TOTAL_BLOQ'] > 0) & (df_completo['DISP'] == 0)
                ,(df_completo['TOTAL_BLOQ'] > 0) & (df_completo['DISP'] > 0)
                ,(df_completo['TOTAL_BLOQ'] == 0) & (df_completo['DISP'] > 0)
            ]
            bloq_result = [
                "TOTAL"
                ,"PARCIAL"
                ,"LIVRE"
            ]

            categoria_cond = [
                (df_completo['DIAS_S'] > 30) & (df_completo['DIAS_E'] > 30) & (df_completo['DISP'] > 0) 
                ,df_completo['DISP'] > 0
                ,df_completo['ESTOQUE'] == 0
            ]
            categoria_result = [
                "PARADO"
                ,"ATIVOS"
                ,"INATIVO"
            ]

            df_completo['STATUS_BLOQUEIO'] = np.select(bloq_quest, bloq_result, default= "--")
            df_completo['CATEGORIAS'] = np.select(categoria_cond, categoria_result, default= "BLOQUEADO")

            df_completo = df_completo.merge(grupo_AE, left_on= 'CODPROD', right_on= "COD", how= 'left').drop(columns='COD')
            df_completo = df_completo.sort_values(by= ['RUA', 'PREDIO', 'APTO'])

            df_ativos = df_completo.loc[df_completo['OBS2'] =="ATIVO"]
            df_FL = df_completo.loc[df_completo['OBS2'] =="FL"]
            pass
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            with pd.ExcelWriter(self.saida) as destino:
                df_ativos.to_excel(destino, sheet_name= "ATIVOS", index= False)
                df_FL.to_excel(destino, sheet_name= "FLs", index= False)
                df_completo.to_excel(destino, sheet_name= "COMPLETO", index= False)


            return True
        except Exception as e:
            self.validar_erro(e, "Load")
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
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False

if __name__ == "__main__":
    Giro_Status()