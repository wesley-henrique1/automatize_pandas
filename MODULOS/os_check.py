from modulos._settings import Relatorios, ColNames, Wms, Outros, Output
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
            f"FONTE: os_check.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )

        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")
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
class Os_check(auxiliar):
    def __init__(self):
        self.list_path = [Outros.ou_func, Wms.wms_07_end, Relatorios.rel_28]

        hoje = dt.datetime.now()
        self.ontem =hoje - dt.timedelta(days=1)
        self.ontem = self.ontem.date()

    def pipeline(self):
        try:
            col_28 = ['DATA','NUMOS','CODPROD', 'DESCRICAO', 'ENDERECO_ORIG', 'RUA', 'PREDIO', 'NIVEL', 'APTO','RUA_1', 'PREDIO_1','NIVEL_1', 'APTO_1', 'QT', 'POSICAO', 'CODFUNCOS', 'CODFUNCESTORNO', 'Tipo O.S.', 'FUNCOSFIM','FUNCGER']

            df_func = pd.read_excel(self.list_path[0], sheet_name= 'FUNC', usecols= ['ID_FUNC', 'SETOR'])
            df_aereo = pd.read_csv(self.list_path[1], header= None, names= ColNames.col_end)
            df_28 = pd.read_excel(self.list_path[2], usecols= col_28)
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        try:
            df_28['DATA'] = pd.to_datetime(df_28['DATA'])
            df_28 = df_28.loc[df_28['DATA'].dt.date == self.ontem]
            
            df_28[['CODFUNCESTORNO', 'CODFUNCOS']] = df_28[['CODFUNCESTORNO', 'CODFUNCOS']].fillna(0).astype(int)
            df_28 = df_28.loc[(df_28['NIVEL'].between(2,8)) & (df_28['RUA'] < 40)  & (df_28['CODFUNCESTORNO'] == 0)  & (df_28['RUA_1'] != 50) & (df_28['Tipo O.S.'] == '58 - Transferencia de Para Vertical')]
            df_28['ID_COD'] = df_28['ENDERECO_ORIG'].astype(str) + " - " + df_28['CODPROD'].astype(str)

            df_aereo = df_aereo.loc[df_aereo['TIPO_PK'] == 'AE']
            df_aereo = df_aereo[['COD_END', 'COD', 'QTDE', 'ENTRADA', 'SAIDA']]
            df_aereo['ID_COD'] = df_aereo['COD_END'].astype(str) + " - " + df_aereo['COD'].astype(str)

            df_baixa = df_28.merge(df_aereo, on= 'ID_COD', how= 'left').fillna(0).drop(columns=['COD_END'])
            df_baixa = df_baixa.merge(df_func, left_on= 'CODFUNCOS', right_on= 'ID_FUNC', how= 'left').drop(columns=['ID_FUNC'])
            df_baixa['SETOR'] = df_baixa['SETOR'].fillna("FUNC")  

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
            df_pd = df_pd[['NUMOS','CODPROD','DESCRICAO','RUA','PREDIO','APTO', 'RUA_1','PREDIO_1','APTO_1','FUNCGER','CATEGORIA','COD']]
            df_pd = df_pd.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)

            col_fim = ['RECEBIMENTO','EMPILHADOR','ABASTECEDOR']
            df_fim = df_baixa.loc[(~df_baixa['SETOR'].isin(col_fim)) & (df_baixa['CODFUNCOS'] > 0) & (df_baixa['QTDE'] == 0)]
            df_fim = df_fim[['NUMOS','CODPROD','DESCRICAO','RUA','PREDIO','APTO','RUA_1','PREDIO_1','APTO_1','FUNCOSFIM','CATEGORIA','COD']]
            df_fim = df_fim.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            with pd.ExcelWriter(Output.rel_os) as destino:
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
