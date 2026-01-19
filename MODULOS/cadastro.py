from modulos._settings import Relatorios, Outros, Output
import datetime as dt
import pandas as pd
import numpy as np
import warnings
import os
import re
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
            f"FONTE: cadastro.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")
    def extrair_e_converter_peso(self,argumento):
        match = re.search(r'([\d\.,]+)\s*(KG|GR)', str(argumento), re.IGNORECASE)
        if match:
            valor_str = match.group(1).replace(',', '.')
            valor = float(valor_str)
            unidade = match.group(2).upper()
            if unidade == 'KG':
                return valor * 1000
            elif unidade == 'GR':
                return valor     
        return None

class Cadastro(auxiliar):
    def __init__(self):
        self.lista_files = [Relatorios.rel_96, Outros.ou_end]
        self.chekout = [27, 28, 29, 31, 32, 33, 34, 35, 36, 37, 38, 39, 44]
        self.list_int = ['2-INTEIRO(1,90)', '1-INTEIRO (2,55)']
        
        largura = 100
        comprimento = 120
        self.area_pl = (largura * comprimento) + 100

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
            colunas_origem = [
                "RUA"
                ,"PREDIO"
                ,"CODPROD"
                ,"OBS2"
                ,"ABASTECEPALETE"
                ,"CAPACIDADE"
                ,"PONTOREPOSICAO"
                ,"QTUNITCX"
                ,"LARGURAARM"
                ,"COMPRIMENTOARM"
                ,"LASTROPAL"
                ,"PK_END"
                ,"DESCRICAO"
                ,"QTTOTPAL"
                ,"CARACTERISTICA"
                ,"APTO"
                ,"TIPO_1"
            ]
            dados_prod = pd.read_excel(self.lista_files[0], usecols= colunas_origem)
            endereco = pd.read_excel(self.lista_files[1], sheet_name= 'STATUS', usecols= ["RUA", "TIPO_RUA", "CARACT"])
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        try:
            TEMP = dados_prod.merge(endereco, on= 'RUA', how= 'inner')
            df_prod = TEMP.loc[TEMP['RUA'].between(1,39)].copy()
            concat = df_prod['RUA'].astype(str) + " - " + df_prod['PREDIO'].astype(str)

            dic_raname = {
                'ABASTECEPALETE' : 'FLEG_ABST',
                'CAPACIDADE' : 'CAP',
                'PONTOREPOSICAO' : 'P_REP',
                'QTUNITCX' : 'FATOR'
            }
            df_prod = df_prod.rename(columns=dic_raname)

            df_prod['AREA_LT'] = round((df_prod['LARGURAARM'] * df_prod['COMPRIMENTOARM']) * df_prod['LASTROPAL'],0)
            df_prod['CONT_AP'] = concat.map(concat.value_counts())
            df_prod['STATUS_PROD'] = np.where(
                (df_prod['CONT_AP'] <= 2) & (df_prod['PK_END'].isin(self.list_int))
                ,"INT"
                , np.where(
                    ( df_prod['CONT_AP'] > 3) & (~df_prod['PK_END'].isin(self.list_int))
                    ,"DIV"
                    ,"VAL"
                )
            )
            df_prod['GRAMATURA_GR'] = df_prod["DESCRICAO"].apply(self.extrair_e_converter_peso).fillna(0)
            
            try:
                df_prod["VAL_CAP"] = np.where(
                    (df_prod['CAP'] < df_prod['QTTOTPAL']) & (df_prod['STATUS_PROD'] == "INT")
                    ,"DIVERGENCIA"
                    ,np.where(
                        (df_prod['CAP'] > df_prod['QTTOTPAL']) & (df_prod['STATUS_PROD'] == "DIV")
                        ,"DIVERGENCIA"
                        ,"NORMAL"
                    )
                )
                df_prod['VAL_FLEG'] = np.where(
                    (df_prod['FLEG_ABST'] == 'SIM') & (df_prod['STATUS_PROD'] == "INT")
                    ,"NORMAL"
                    ,np.where(
                        (df_prod['FLEG_ABST']== 'NÃO') & (df_prod['STATUS_PROD'] == "DIV")
                        ,"NORMAL"
                        ,"DIVERGENCIA"
                    )
                )
                df_prod['VAL_CUBAGEM'] = np.where(
                    df_prod['AREA_LT'] > self.area_pl
                    ,"DIVERGENCIA"
                    ,"NORMAL"
                )
                df_prod['VAL_CARACT'] = np.where(
                    df_prod['CARACTERISTICA'] != df_prod['CARACT']
                    ,"DIVERGENCIA"
                    ,"NORMAL"
                )
                df_prod['VAL_PESO'] = np.where(
                    (df_prod["GRAMATURA_GR"] >= 1000) & (df_prod['APTO'] > 200) & (~df_prod['RUA'].isin([31,32]))
                    ,"ACIMA DA BANDEJA"
                    ,"NORMAL"
                )
                df_prod['VAL_TIPO_OS'] = np.where(
                    (df_prod['TIPO_1'] == '1 - GRANDEZA') & (df_prod['RUA'].isin(self.chekout))
                    ,"DIVERGENCIA"
                    ,"NORMAL"
                )
                df_prod['VAL_RUA'] = np.where(
                    (df_prod['TIPO_RUA'] == 'UN') & (df_prod['FATOR'] == 1)
                    ,"DIVERGENCIA",
                    np.where(
                        (df_prod['TIPO_RUA'] == 'CX') & (df_prod['FATOR'] != 1)
                        ,"DIVERGENCIA"
                        ,"NORMAL"
                    )
                )
            except Exception as e:
                self.validar_erro(e, "T-coluna calculadas")
                return False

            try:
                ordem_primaria = ['CODPROD', 'DESCRICAO','OBS2', 'RUA', 'PREDIO', 'APTO', 'TIPO_RUA','CARACTERISTICA']
                capacidade = ['FATOR','QTTOTPAL','CAP', 'P_REP','CONT_AP','FLEG_ABST','STATUS_PROD']
                validar = ['VAL_CAP', 'VAL_FLEG', 'VAL_CARACT', 'VAL_TIPO_OS', 'VAL_CUBAGEM', 'VAL_PESO', 'VAL_RUA']
                ordem_completa = ordem_primaria + capacidade + validar
                df_final = df_prod[ordem_completa]

                contagem = df_final['RUA'].nunique()
                misto = df_final['RUA'].loc[df_final['TIPO_RUA'] == 'MISTO'].nunique()
                caixa = df_final['RUA'].loc[df_final['TIPO_RUA'] == 'CX'].nunique()
                unitario = df_final['RUA'].loc[df_final['TIPO_RUA'] == 'UN'].nunique()

                pront_tt = df_final['CODPROD'].nunique()
                prod_caixa = df_final['CODPROD'].loc[df_final['FATOR'] == 1].nunique()
                prod_unitario = df_final['CODPROD'].loc[df_final['FATOR'] != 1].nunique()
                porcent_cx = round((prod_caixa / pront_tt) * 100, 2)
                porcent_un = round((prod_unitario / pront_tt) * 100, 2)

                df_amostradinho = pd.DataFrame({
                    "CATEGORIA": ["RUAS", "PRODUTOS", "PERC_PROD (%)"],
                    "CONTAGEM":  [contagem, pront_tt, 100],
                    "MISTO":     [misto, 0, 0],
                    "CAIXA":     [caixa, prod_caixa, porcent_cx],
                    "UNITARIO":  [unitario, prod_unitario, porcent_un],
                    "x":         ["x", "x", "x"]
                })
            except Exception as e:
                self.validar_erro(e, "T-organizar")
                return False
        except Exception as e:
                self.validar_erro(e, "Transform")
                return False
        try:
            df_final = df_final.sort_values(by=['RUA', 'PREDIO'], ascending= True)
            with pd.ExcelWriter(Output.cadastro) as PL:
                df_final.to_excel(PL, sheet_name= "cadastro", index= False)
                df_amostradinho.to_excel(PL, sheet_name= "demostrativo", index= False)
            return True
        except Exception as e:
            self.validar_erro(e, "Load")
            return False
