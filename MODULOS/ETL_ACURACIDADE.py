from MODULOS.config_path import directory, outros, wms, relatorios, col_names
import datetime as dt
import pandas as pd
import numpy as np
import os
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

class auxiliar:
    def validar_erro(self, e, etapa):
        LARGURA = 78
        if isinstance(e, PermissionError):
            msg = (
                f">>> O arquivo de destino está aberto ou você não tem permissão."
                f">>> Por favor, feche o Excel e tente novamente."
            )      
        elif isinstance(e, FileNotFoundError):
            msg = (
                f">>> Um dos arquivos de origem não foi encontrado."
                f">>> Verifique se a pasta 'base_dados' está ao lado do executável."
            )
        elif isinstance(e, KeyError):
            msg = (f">>> A coluna ou chave '{e}' não foi encontrada no DataFrame.")           
        elif isinstance(e, TypeError):
            msg = (
                f">>> Erro de tipo: Operação inválida entre dados incompatíveis."
                f">>> Detalhe: {e}"
            )     
        elif isinstance(e, ValueError):
            msg = (
                f">>> Erro de valor: O formato do dado não corresponde ao esperado."
                f">>> Detalhe: {e}"
            )
        elif isinstance(e, NameError):
            msg = (
                f">>> Erro de definição: Variável ou função não definida."
                f">>> Detalhe: {e}"
            )
        else:
            msg = (f">>> Erro não mapeado: {e}")
        agora = dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        log_conteudo = (
            f"{'='* LARGURA}\n"
            f"FONTE:ETL_ACURACIDADE.py.py"
            f"ETAPA: {etapa} - {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* LARGURA}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as erros_log:
                erros_log.write(log_conteudo)
        except Exception as erro_gravacao:
            print(f"Não foi possível gravar o log: {erro_gravacao}")
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


class acuracidade:
    def __init__(self):
        self.lista_files = [outros.ou_86, wms.wms_07_ger,wms.wms_07_end,relatorios.rel_96]
        print(f"{"ACURACIDADE":_^78}")
        self.pipeline() 

    def carregamento(self):
        lista_de_logs = []
        try:
            for contador, path in enumerate(self.lista_files, 1):
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
            return lista_de_logs
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False

    def pipeline(self):
        try:
            df_bloq = pd.read_excel(self.lista_files[0], usecols=['Código', 'Bloqueado(Qt.Bloq.-Qt.Avaria)'])
            end_ger = pd.read_csv(self.lista_files[1], header= None, names= col_names.col_ger)  
            df_end = pd.read_csv(self.lista_files[2], header= None, names= col_names.col_end)
            df_prod = pd.read_excel(self.lista_files[3], usecols= ['CODPROD', 'QTUNITCX','QTTOTPAL'])
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(f"Etapa extração: {self.erro}")
            
        try:        
            dic_end_ger = {'COD' : "CODPROD"}
            end_ger = end_ger.rename(columns= dic_end_ger)
            end_ger['RUA'] = end_ger['RUA'].fillna(0).astype(int)

            dic_bloq = {'Código' : "CODPROD", 'Bloqueado(Qt.Bloq.-Qt.Avaria)' : "BLOQ"}
            df_bloq = df_bloq.rename(columns= dic_bloq)
            df_bloq['CODPROD'] = df_bloq['CODPROD'].fillna(0).astype(int)

            dic_end = {"COD" : "CODPROD"}
            df_end = df_end.rename(columns= dic_end)
            df_end = df_end[['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']]
            
            col_ajuste = ['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']
            for col in col_ajuste:
                df_end = Funcao.ajustar_numero(df_end, col, int)
            
            grupo_end = df_end.groupby('CODPROD').agg(
                SAIDA = ('SAIDA', 'sum'),
                ENTRADA = ('ENTRADA', 'sum'),
                DISP = ('DISP', 'sum'),
                QTDE = ('QTDE', 'sum'),
            ).reset_index()

            df_prod['CODPROD'] = df_prod['CODPROD'].fillna(0).astype(int)

            df = end_ger.loc[end_ger['RUA'].between(1, 39)]
            df = df.merge(df_bloq, left_on='CODPROD',right_on='CODPROD', how= 'left')
            df = df.merge(grupo_end, left_on='CODPROD',right_on='CODPROD', how= 'left')
            df = df.merge(df_prod, left_on='CODPROD',right_on='CODPROD', how= 'left')
            drop_col = ['EMBALAGEM']
            df.drop(columns= drop_col, inplace= True)

            col_ajuste = ['ENDERECO', 'GERENCIAL','PICKING','CAP','QTUNITCX','ENTRADA', 'SAIDA', 'QTDE']
            for col in col_ajuste:
                df = Funcao.ajustar_numero(df, col, float)

            df['DIF_UN'] = df['ENDERECO'].astype(float) - df['GERENCIAL'].astype(float)
            df['DIF_CX'] = (df['DIF_UN'] / df['QTUNITCX'].astype(int)).round(1)
            df['ENDERECO'] = df['ENDERECO'] + df['ENTRADA']
            df['CAP_CONVERTIDA'] = df['CAP'] * df['QTUNITCX']
            df['PENDENCIA'] = np.where(df['QTDE_O.S'] > 0, 'FOLHA', 'INVENTARIO')

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
            result_dif = ['0', '1', '2', '3', '-1', '-2', '-3',]

            cond_op = [
                df['DIF_UN'] < 0,
                df['DIF_UN'] > 0,
                df['DIF_UN'] == 0,
            ]
            result_op = ["END_MENOR", "END_MAIOR", "CORRETO"]

            cond_ap = [
                df['PICKING'].astype(int) > df['CAP_CONVERTIDA'].astype(int),
                df['PICKING'].astype(int) < 0,
                df['PICKING'].astype(int) == 0
            ]
            result_ap = [
                'AP_MAIOR',
                "AP_NEGATIVO",
                "CORRETO"
            ]

            df['CATEGORIA_DIF'] = np.select(cond_dif, result_dif, default= "99").astype(int)
            df["TIPO_OP"] = np.select(cond_op, result_op, default= "-")
            df['AP_VS_CAP'] = np.select(cond_ap, result_ap, default= "-")
            df[['RUA', 'PREDIO']] = df[['RUA', 'PREDIO']].astype(int)
            df_prod = df_prod.drop_duplicates(subset=None, keep='first')
            df = df.sort_values(by=['RUA', 'PREDIO'], ascending= True)
        except Exception as e:
            erro = Funcao.validar_erro(e)
            print(f"Etapa tratamento: {erro}")

        try:
            dic = directory.dir_acuracidade
            path_div = os.path.join(dic, "FATO_DIVERGENCIA.xlsx")
            path_prod = os.path.join(dic, "DIM_PROD.xlsx")

            df.to_excel(path_div, index= False, sheet_name= 'DIVERGENCIA')
            df_prod.to_excel(path_prod, index= False, sheet_name= 'DIM_PROD')
        except Exception as e:
            erro = Funcao.validar_erro(e)
            print(f"Etapa carga: {erro}")

if __name__ == '__main__':
    acuracidade()
    input("Aperte 'enter' para finalizar o processo...")