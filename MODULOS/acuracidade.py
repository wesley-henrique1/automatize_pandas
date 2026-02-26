from modulos._settings import Directory, Outros, Wms, Relatorios, ColNames
import datetime as dt
import pandas as pd
import numpy as np
import os
import warnings
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
            f"FONTE: acuracidade.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as e:
            print(f"Falha crítica ao gravar log: {e}")
            self.validar_erro(e, "CARREGAMENTO")
            return False
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
    def ajustar_numero(self,df_copia, coluna, tipo_dados):
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
class Acuracidade(auxiliar):
    def __init__(self):
        self.list_path = [Outros.ou_86, Wms.wms_07_ger,Wms.wms_07_end,Relatorios.rel_96]

    def pipeline(self):
        try:
            df_bloq = pd.read_excel(self.list_path[0], usecols=['Código', 'Bloqueado(Qt.Bloq.-Qt.Avaria)','Qt.Avaria'])
            end_ger = pd.read_csv(self.list_path[1], header= None, names= ColNames.col_ger)  
            df_end = pd.read_csv(self.list_path[2], header= None, names= ColNames.col_end)
            df_prod = pd.read_excel(self.list_path[3], usecols= ['CODPROD', 'QTUNITCX','QTTOTPAL'])
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        try:
            dic_end_ger = {'COD' : "CODPROD"}
            end_ger = end_ger.rename(columns= dic_end_ger)
            end_ger['RUA'] = end_ger['RUA'].fillna(0).astype(int)

            dic_bloq = {'Código' : "CODPROD", 'Bloqueado(Qt.Bloq.-Qt.Avaria)' : "BLOQ", "Qt.Avaria": "AVARIA"}
            df_bloq = df_bloq.rename(columns= dic_bloq).fillna(0)
            df_bloq["BLOQUEADOS"] = df_bloq['BLOQ'] + df_bloq['AVARIA']
            df_bloq['CODPROD'] = df_bloq['CODPROD'].astype(int)

            dic_end = {"COD" : "CODPROD"}
            df_end = df_end.rename(columns= dic_end)
            df_end = df_end.loc[df_end['TIPO_PK'] == "AE"]
            df_end = df_end[['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']]
            
            col_ajuste = ['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']
            for col in col_ajuste:
                df_end = self.ajustar_numero(df_end, col, int)
            
            grupo_end = df_end.groupby('CODPROD').agg(
                SAIDA = ('SAIDA', 'sum'),
                ENTRADA = ('ENTRADA', 'sum'),
                QT_DISP = ('DISP', 'sum'),
                QTDE = ('QTDE', 'sum'),
            ).reset_index()

            df_prod['CODPROD'] = df_prod['CODPROD'].fillna(0).astype(int)

            df = end_ger.loc[end_ger['RUA'].between(1, 39)]
            df = df.merge(df_bloq, left_on='CODPROD',right_on='CODPROD', how= 'left')
            df = df.merge(grupo_end, left_on='CODPROD',right_on='CODPROD', how= 'left')
            df = df.merge(df_prod, left_on='CODPROD',right_on='CODPROD', how= 'left')
            drop_col = ['EMBALAGEM']
            df.drop(columns= drop_col, inplace= True)

            col_ajuste = ['ENDERECO', 'GERENCIAL','PICKING','CAP','QTUNITCX','ENTRADA', 'SAIDA', 'QTDE', 'QT_DISP']
            for col in col_ajuste:
                df = self.ajustar_numero(df, col, float)


            df['DIF_UN'] = df['ENDERECO'] - df['GERENCIAL']
            df['DIF_CX'] = round(df['DIF_UN'] / df['QTUNITCX'], 1)
            df['ENDERECO'] = df['ENDERECO'] + df['ENTRADA']
            df['CAP_CONVERTIDA'] = df['CAP'] * df['QTUNITCX']
            df["STG_DISP"] =  df['GERENCIAL'] - df['BLOQUEADOS']
            

            for col in ['PICKING', 'CAP_CONVERTIDA', 'DIF_CX', 'DIF_UN','RUA', 'PREDIO', 'STG_DISP']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            # CRIAÇÃO DAS COLUNAS CATEGORIZADAS

            df['VAL_ESTOQUE'] = np.where(
                df['STG_DISP'].between(1, 40)
                ,"VALIDAR"
                ,"NORMAL"
            )
            df['PENDENCIA'] = np.where(
                df['QTDE_O.S'] > 0
                ,'FOLHA'
                ,'INVENTARIO'
            )

            cond_dif = [
                df['DIF_CX'] == 0,
                # POSITIVO
                (df['DIF_CX'] > 0) & (df['DIF_CX'] < 5),
                (df['DIF_CX'] >= 5) & (df['DIF_CX'] < 10),
                df['DIF_CX'] >= 10,
                # NEGATIVO
                (df['DIF_CX'] < 0) & (df['DIF_CX'] > -5),
                (df['DIF_CX'] <= -5) & (df['DIF_CX'] > -10),
                df['DIF_CX'] <= -10
            ]
            result_dif = [0, 1, 2, 3, -1, -2, -3]
            df['CATEGORIA_DIF'] = np.select(cond_dif, result_dif, default=99)

            cond_op = [
                df['DIF_UN'] < 0,
                df['DIF_UN'] > 0,
                df['DIF_UN'] == 0,
            ]
            result_op = ["END_MENOR", "END_MAIOR", "CORRETO"]
            df["TIPO_OP"] = np.select(cond_op, result_op, default= "-")

            cond_ap = [
                df['PICKING'] > df['CAP_CONVERTIDA'],
                df['PICKING'] < 0,
                df['PICKING'] == 0
            ]
            result_ap = [
                'AP_MAIOR'
                ,"AP_NEGATIVO"
                ,"CORRETO"
            ]
            df['AP_VS_CAP'] = np.select(cond_ap, result_ap, default= "-")
            
            df_prod = df_prod.drop_duplicates(subset=None, keep='first')
            df = df.sort_values(by=['RUA', 'PREDIO'], ascending= True)
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            dic = Directory.dir_acuracidade
            path_div = os.path.join(dic, "FATO_DIVERGENCIA.xlsx")
            path_prod = os.path.join(dic, "DIM_PROD.xlsx")

            df.to_excel(path_div, index= False, sheet_name= 'DIVERGENCIA')
            df_prod.to_excel(path_prod, index= False, sheet_name= 'DIM_PROD')
            return True
        except Exception as e:
            self.validar_erro(e, "Load")
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
            
            dic_retorno = []
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False

