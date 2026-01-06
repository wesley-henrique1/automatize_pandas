import sys
caminho_env = r'C:\Users\wesley.oliveira\WS_OLIVEIRA\SCRIPTS\.meu_ambiente\Lib\site-packages'
if caminho_env not in sys.path:
    sys.path.insert(0, caminho_env)
from MODULOS.config_path import directory, DB_acumulado
from config.fuction import Funcao
import pandas as pd
import glob
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


class cheio_vazio:
    def __init__(self):
        DRIVER = DB_acumulado.drive
        DB_ACUMULADO = DB_acumulado.path_acumulado
        self.NOME_TABELA = DB_ACUMULADO.tb_vz_ch

        self.ODBC_CONN_STR = (
            f"DRIVER={DRIVER};"
            f"DBQ={DB_ACUMULADO};"
        )

        self.dados = self.pipeline()


    def conectar_db(self, tabela):
        query_select = f"SELECT NOME_RELATORIO FROM {tabela}"

        try:
            connection_url = URL.create(
            "access+pyodbc",
            query={"odbc_connect":  self.ODBC_CONN_STR}
            )
            self.engine = create_engine(connection_url)
            with self.engine.connect() as connection:
                result = connection.execute(text(query_select))
                dados_existentes = result.fetchall()
                print("Conectado com sucesso ao Access!")
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(error)
            exit()
            
        return dados_existentes
    def atualizar(self, df, tabela):
        try:            
            df.to_sql(
                name= tabela,
                con= self.engine,
                if_exists='append',
                index=False,
            )
            print(f"DataFrame salvo com sucesso na tabela '{tabela}' no Access.")
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(error)
            exit()

    def pipeline(self):
        try:  # EXTRAÇÃO DOS DADOS
            dados_db = self.conectar_db(self.NOME_TABELA)

            pasta_files = glob.glob(os.path.join(directory.dir_cheio_vazio, "*xls*"))

            list_processado = []
            list_files_db = []
            list_erros = {}
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(error)
            exit()

        try: # TRATAMENTO DOS DADOS
            arquivos_processados = set([tupla[0] for tupla in dados_db])
            
            for file in pasta_files:
                NAME_FILE = os.path.basename(file)
                if NAME_FILE in arquivos_processados:
                    list_files_db.append(NAME_FILE)
                    continue
                try:
                    df = pd.read_excel(file, header= 2)
                    df_filtrado = df.loc[df['Retorno'].isin(['CHEIO', 'VAZIO'])]
                    df_filtrado = df_filtrado[['END', 'COD', 'DESCRIÇÃO', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'Retorno', 'data_relatorio']]
                    df_filtrado['NAME_FILE'] = NAME_FILE
                    list_processado.append(df_filtrado)
                except Exception as e:
                    list_erros['ETAPA_CONSOLIDADO'] = str(NAME_FILE), str(e)

            df_consolidado = pd.concat(list_processado, axis= 'index')

            list_renomear = {'DESCRIÇÃO':'DESC', 'data_relatorio':'DATA_RELATORIO', 'NAME_FILE':'NOME_RELATORIO', 'Retorno':'RETORNO'}
            df_consolidado = df_consolidado.rename(columns=list_renomear)

            col_int = ['END', 'COD','RUA','PREDIO','NIVEL','APTO']
            for col in col_int:
                try:
                    df_consolidado[col] = df_consolidado[col].fillna(0).astype(int)
                except Exception as e:
                    list_erros['LIMPEZA'] = str(col),str(e)

        except Exception as e:
            error = Funcao.validar_erro(e)
            print(error)
            exit()

        try:
            LARGURA = 50 

            print("\n" + "=" * LARGURA)
            print(f"{'RELATÓRIO DE PROCESSAMENTO':^{LARGURA}}")
            print("=" * LARGURA)

            print(f"{'ESTATÍSTICAS DE ARQUIVOS':^{LARGURA}}")
            print("-" * LARGURA)
            print(f"| {'Arquivos Processados:':<30}| {len(list_processado):>15}|")
            print(f"| {'Registros Salvos no Banco:':<30}| {len(list_files_db):>15}|")
            print("-" * LARGURA)

            if list_erros:
                print(f"{'RELATÓRIO DE FALHAS':^{LARGURA}}")
                print("-" * LARGURA)
                for etapa, detalhe_erro in list_erros.items():
                    nome_arquivo = detalhe_erro[0]
                    descricao_erro = detalhe_erro[1]
                    print(f"ETAPA DA FALHA: {etapa}")
                    print(f"  > Arquivo Afetado: {nome_arquivo}")
                    print(f"  > Causa: {descricao_erro}")
                    print("-" * LARGURA)
            else:
                print(f"{'STATUS: PROCESSAMENTO CONCLUÍDO SEM ERROS':^{LARGURA}}")

            print(f"\n{'ATUALIZAÇÃO DE DADOS INICIADA':^{LARGURA}}")
            print(f"Tentando salvar {len(list_files_db)} registros na tabela '{self.NOME_TABELA}'...")
            print("-" * LARGURA + "\n")

            self.atualizar(df_consolidado, self.NOME_TABELA)
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(error)


if __name__ == '__main__':
    cheio_vazio()
    input("Aperte 'enter' para finalizar o processo...")