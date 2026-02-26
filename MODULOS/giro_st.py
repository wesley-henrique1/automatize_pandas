from modulos._settings import Relatorios, Outros, Output, ColNames, Wms
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
    def organizar_df(self, df_original, col, id):
        df = df_original
        df[col] = pd.to_datetime(df[col]).dt.normalize()
        df['MES'] = df[col].dt.month

        if id == 1:
            df = df[['NUMOS', 'DATA', 'CODROTINA', 'POSICAO', 'CODFUNCGER', 'FUNCGER','DTFIMOS', 'CODFUNCOS', 'FUNCOSFIM', 'Tipo O.S.', 'TIPOABAST', 'MES']].copy()
            df = df.loc[(df['CODROTINA'] == 1709) | (df['CODROTINA'] == 1723)]
            return df
        elif id ==2: 
            df = df[['DATAGERACAO', 'DTLANC', 'NUMBONUS', 'NUMOS', 'CODEMPILHADOR','EMPILHADOR', 'MES']].copy()
            df = df.drop_duplicates(subset=['NUMOS'])
            return df
    def agrupar(self, df, col, id):
        if id == 1:
            temp = df.groupby(col).agg(
                QTDE_OS = ("NUMOS", 'nunique'),
                OS_50 = ("Tipo O.S.", lambda x: (x == '50 - Movimentação De Para').sum()),
                OS_61 = ("Tipo O.S.", lambda x: (x == '61 - Movimentação De Para Horizontal').sum()),
                OS_58 = ("Tipo O.S.", lambda x: (x == '58 - Transferencia de Para Vertical').sum())
            ).reset_index()
            return temp
        elif id == 2:
            temp = df.groupby(col).agg(
                OS_RECEB = ("NUMOS", 'nunique')
            ).reset_index()
            return temp
        elif id == 3:
            temp = df.groupby(col).agg(
                OS_RECEB = ('OS_RECEB', 'sum')
            ).reset_index()
            temp[col] = pd.to_datetime(temp[col]).dt.normalize()
            return temp
        elif id == 4:
            temp = df.groupby(col).agg(
                QTDE_OS = ("QTDE_OS", 'sum'),
                OS_50 = ("OS_50", 'sum'),
                OS_61 = ("OS_61", 'sum'),
                OS_58 = ("OS_58", 'sum')
            ).reset_index()
            temp[col] = pd.to_datetime(temp[col]).dt.normalize()
            return temp 
        elif id == 5:
            temp = df.groupby([col]).agg(
                    TOTAL_BONUS = ("NUMBONUS", "nunique")
                ).reset_index().sort_values(by=col, ascending= False)  
            return temp 
class Giro_Status(auxiliar):
    def __init__(self):
        self.HOJE = dt.datetime.now()
        self.list_path = [Relatorios.rel_96,Relatorios.rel_f18,Outros.ou_86, Outros.ou_f18,Wms.wms_07_end, Outros.ou_end]

    def pipeline(self):
        try:
            col_96 = [
                'CODPROD', 
                'DESCRICAO', 
                'OBS2', 
                'RUA', 
                'PREDIO', 
                'NIVEL', 
                'APTO', 
                'QTESTGER', 
                'DTULTENT', 
                'DTULTSAIDA'
            ]
            PRODUTO_F11 = pd.read_excel(self.list_path[0], usecols= col_96)
            PRODUTO_F18 = pd.read_excel(self.list_path[1], usecols= col_96)

            colunas_estoque = [
                'Código', 
                'Estoque', 
                'Qtde Pedida', 
                'Bloqueado(Qt.Bloq.-Qt.Avaria)', 
                'Qt.Avaria', 
                'Custo real', 
                'Comprador'
            ]
            ESTOQUE_F11 = pd.read_excel(self.list_path[2], usecols= colunas_estoque)
            ESTOQUE_F18 = pd.read_excel(self.list_path[3], usecols= colunas_estoque)

            enderecado = pd.read_csv(self.list_path[4], header= None, names= ColNames.col_end)
            BASE_END = pd.read_excel(self.list_path[5], sheet_name= "AE", usecols= ['COD_END', 'RUA'])
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        try:
            try:
                for col in ['DTULTENT','DTULTSAIDA']: # AJUSTAR DATAS
                    PRODUTO_F11[col] = pd.to_datetime(PRODUTO_F11[col], errors= 'coerce')
                    PRODUTO_F18[col] = pd.to_datetime(PRODUTO_F18[col], errors= 'coerce')

                PRODUTO_F18 = PRODUTO_F18.drop(columns=['DESCRICAO','OBS2', 'RUA', 'PREDIO', 'NIVEL', 'APTO'])
                df_prod = PRODUTO_F11.merge(PRODUTO_F18, on= 'CODPROD', how= 'left')
                df_prod['DTULTENT'] = df_prod[['DTULTENT_x', 'DTULTENT_y']].max(axis=1)
                df_prod['DTULTSAIDA'] = df_prod[['DTULTSAIDA_x', 'DTULTSAIDA_y']].max(axis=1)
                df_prod['QTESTGER'] = df_prod['QTESTGER_x'].fillna(0) + df_prod['QTESTGER_y'].fillna(0)

                df_prod = df_prod[col_96]
            except Exception as e: 
                self.validar_erro(e, "AJUSTE_PROD")
                return False
            try:
                ESTOQUE_F18 = ESTOQUE_F18[['Código', 'Estoque','Bloqueado(Qt.Bloq.-Qt.Avaria)','Qt.Avaria','Comprador']]
                renomear = {
                    'Código':"COD_F18"
                    ,'Estoque': "EST_F18"
                    ,'Bloqueado(Qt.Bloq.-Qt.Avaria)': 'BLOQ_F18'
                    ,'Qt.Avaria': "AV_F18"
                    ,'Comprador': "COMP_F18"
                }
                df_val = ESTOQUE_F18.rename(columns= renomear)

                df_estoque = ESTOQUE_F11.merge(df_val, left_on= 'Código', right_on= 'COD_F18', how= 'left')
                df_estoque = df_estoque.fillna(0)                
            except Exception as e: 
                self.validar_erro(e, "AJUSTE_ESTOQUE")
                return False
            
            enderecado = enderecado.loc[enderecado['TIPO_PK'] == "AE"]

            df_estoque['Estoque'] += df_estoque['EST_F18']
            df_estoque['Bloqueado(Qt.Bloq.-Qt.Avaria)'] += df_estoque['BLOQ_F18']
            df_estoque['Qt.Avaria'] += df_estoque['AV_F18']  
            df_estoque['BLOQUEADO'] = (df_estoque['Qt.Avaria'].fillna(0) + df_estoque['Bloqueado(Qt.Bloq.-Qt.Avaria)'].fillna(0)).astype(int)
            df_estoque['CUSTO_PROD'] = round(
                df_estoque['Custo real'].fillna(0).astype(float) * df_estoque['Estoque'].fillna(0).astype(float)
                ,2
            ) 
            df_estoque['DISPONIVEL'] = (df_estoque['Estoque'] - df_estoque['BLOQUEADO']).fillna(0).astype(int)
            nomes_separados = df_estoque['Comprador'].str.split()

            total = nomes_separados.str.len()

            df_estoque['COMPRADOR'] = np.where(
                total > 1, 
                (nomes_separados.str[0] + " " + nomes_separados.str[-1]),
                (nomes_separados.str[0])                                
            )
            
            df_prod['DTULTSAIDA'] = pd.to_datetime(df_prod['DTULTSAIDA'], errors= 'coerce')
            df_prod['DTULTENT'] = pd.to_datetime(df_prod['DTULTENT'], errors= 'coerce')

            tempo_saida = self.HOJE - df_prod['DTULTSAIDA']
            tempo_entrada = self.HOJE - df_prod['DTULTENT']

            df_prod['DIAS_PEDENTE'] = tempo_saida.dt.days.fillna(0)
            df_prod['DIAS_ULT_ENTRADA'] = tempo_entrada.dt.days.fillna(0)
            df_prod['OBS2'] = df_prod['OBS2'].fillna("ATIVO")

            ruas_fora = [50, 60, 70, 80, 100]
            df_prod = df_prod.loc[~df_prod['RUA'].isin(ruas_fora)] 
            col_classificar = [
                'Código'
                ,'Estoque'
                ,'Qtde Pedida'
                ,'BLOQUEADO'
                ,'CUSTO_PROD'
                ,'DISPONIVEL'
                ,'COMPRADOR'
                ]
            df_ordenado = df_estoque[col_classificar]   
            df_completo = df_prod.merge(
                df_ordenado
                ,left_on= 'CODPROD'
                ,right_on= 'Código'
                ,how= 'left'
            ).drop(columns= ['Código'])
            df_completo['QTESTGER'] += df_completo['Estoque']
            enderecado = enderecado.merge(
                BASE_END
                ,on='COD_END'
                ,how= 'left'
            )
            cols = list(enderecado.columns)
            cols.insert(1, cols.pop(cols.index('RUA'))) 
            enderecado = enderecado[cols]

            grupo_endereco = enderecado.groupby('COD').agg(
                QT_AE = ('COD_END', 'nunique')
            ).reset_index()
            df_completo = df_completo.merge(
                grupo_endereco
                ,left_on= 'CODPROD'
                ,right_on= 'COD'
                ,how= 'left'
            ).drop(columns= 'COD').fillna({'QT_AE': 0})
            col_classificar = [
                'CODPROD'
                ,'DESCRICAO'
                ,'OBS2'
                ,'RUA'
                ,'PREDIO'
                ,'NIVEL'
                ,'APTO'
                ,'QTESTGER'
                ,'QT_AE'
                ,'Qtde Pedida'
                ,'DTULTENT'
                ,'DTULTSAIDA'
                ,'DIAS_PEDENTE'
                ,'DIAS_ULT_ENTRADA'
                ,'BLOQUEADO'
                ,'CUSTO_PROD'
                ,'DISPONIVEL'
                ,'COMPRADOR'
                ]
            df_completo = df_completo[col_classificar]

            FL_zerado = df_completo.loc[
                (df_completo['OBS2'] == "FL") 
                & (df_completo['QTESTGER'] == 0)].copy()

            FL_ativo = df_completo.loc[
                (df_completo['OBS2'] == "FL") 
                & (df_completo['QTESTGER'] != 0) 
                & (df_completo['DIAS_PEDENTE'] < 30)].copy()

            ativo_zerado = df_completo.loc[
                (df_completo['OBS2'] == "ATIVO") 
                & (df_completo['QTESTGER'] == 0) 
                & (df_completo['DIAS_PEDENTE'] > 30)].copy()

            ger_parado = df_completo.loc[
                (df_completo['QTESTGER'] > 0) 
                & (df_completo['DIAS_PEDENTE'] > 30) 
                & (df_completo['DIAS_ULT_ENTRADA'] > 30)].copy()
            
            todos_cods = pd.concat([
                FL_zerado["CODPROD"],
                FL_ativo["CODPROD"],
                ativo_zerado["CODPROD"],
                ger_parado["CODPROD"]
            ])
            lista_cod_unica = set(todos_cods)
            aereo_filtrado = enderecado.loc[(enderecado['COD'].isin(lista_cod_unica))]
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            lista_dfs = [FL_zerado, FL_ativo, ativo_zerado, ger_parado, aereo_filtrado]
            for i in range(len(lista_dfs)):
                lista_dfs[i] = lista_dfs[i].sort_values(by=['RUA', 'PREDIO'], ascending=True)
            FL_zerado, FL_ativo, ativo_zerado, ger_parado, aereo_filtrado = lista_dfs
            
            with pd.ExcelWriter(Output.controle_fl) as feijao:
                FL_zerado.to_excel(feijao, index= False, sheet_name= "FL_ZERADO")
                FL_ativo.to_excel(feijao, index= False, sheet_name= "FL_ATIVO")
                ativo_zerado.to_excel(feijao, index= False, sheet_name= "ATIVO_ZERADO")
                ger_parado.to_excel(feijao, index= False, sheet_name= 'PROD_PARADO')
                df_completo.to_excel(feijao, index= False, sheet_name= "GERAL")
                aereo_filtrado.to_excel(feijao, index= False, sheet_name= "AEREOS_RELATORIO")
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
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False
