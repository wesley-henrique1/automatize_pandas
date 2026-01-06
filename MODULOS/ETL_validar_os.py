from MODULOS.config_path import relatorios, col_names, wms, outros, output
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
            f"FONTE: validar_os.py"
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
    def ajustar_numero(df_copia, coluna, tipo_dados):
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

class validar_os(auxiliar):
    def __init__(self):
        self.lista_files = [outros.ou_func, wms.wms_07_end, relatorios.rel_28]

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
            col_28 = ['NUMOS','CODPROD', 'DESCRICAO', 'ENDERECO_ORIG', 'RUA', 'PREDIO', 'NIVEL', 'APTO','RUA_1', 'PREDIO_1','NIVEL_1', 'APTO_1', 'QT', 'POSICAO', 'CODFUNCOS', 'CODFUNCESTORNO', 'Tipo O.S.', 'FUNCOSFIM','FUNCGER']

            df_func = pd.read_excel(self.lista_files[0], sheet_name= 'FUNC', usecols= ['ID_FUNC', 'SETOR'])
            df_aereo = pd.read_csv(self.lista_files[1], header= None, names= col_names.col_end)
            df_28 = pd.read_excel(self.lista_files[2], usecols= col_28)
        except Exception as e:
            self.validar_erro(e, "EXTRAIR")
            return False

        try:
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
            self.validar_erro(e, "TRATAMENTO")
            return False

        try:
            with pd.ExcelWriter(output.rel_os) as destino:
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
        except Exception as e:
            self.validar_erro(e, "CARGA")
            return False


if __name__ == "__main__":
    validar_os()
    input("\nPressione Enter para sair...")