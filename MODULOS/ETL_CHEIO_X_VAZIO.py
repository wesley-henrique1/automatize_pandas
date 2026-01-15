from MODULOS.config_path import Directory, DB_acumulado
import datetime as dt
import pandas as pd
import glob
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

class auxiliar:
    def validar_erro(self, e, etapa):
        largura = 78
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
            f"FONTE: cheio_vazio | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )

        try:
            print("Verificar log de erro")
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")
    def cosultar_db(self, consulta):
        try:
            connection_url = URL.create(
            "access+pyodbc",
            query={"odbc_connect":  self.ODBC_CONN_STR}
            )
            self.engine = create_engine(connection_url)
            with self.engine.connect() as connection:
                result = connection.execute(text(consulta))
                dados_existentes = result.fetchall()

                nomes_colunas = result.keys()
                db_dados = pd.DataFrame(
                    dados_existentes,
                    columns=nomes_colunas
                )
        except Exception as e:
            self.validar_erro(e, "Consulta_banco_dados")
            return pd.DataFrame()

        return db_dados        
    def atualizar(self, df, tabela):
        try:            
            df.to_sql(
                name= tabela,
                con= self.engine,
                if_exists='append',
                index=False,
            )
        except Exception as e:
            self.validar_erro(e, "Atualizar_banco_dados")
            return False

class cheio_vazio(auxiliar):
    def __init__(self):
        DRIVER = DB_acumulado.drive
        self.DB_ACUMULADO = DB_acumulado.path_acumulado
        self.NOME_TABELA = DB_acumulado.tb_vz_ch

        self.ODBC_CONN_STR = (
            f"DRIVER={DRIVER};"
            f"DBQ={self.DB_ACUMULADO};"
        )
        self.pipeline()

    def pipeline(self):
        try:  # EXTRAÇÃO DOS DADOS
            query_select = f"SELECT {'NOME_RELATORIO'} FROM {self.NOME_TABELA}"
            names_db = self.cosultar_db(query_select)
            arquivos_processados = set(names_db['NOME_RELATORIO'].str.strip().tolist())

            pasta_files = glob.glob(os.path.join(Directory.dir_cheio_vazio, "*xls*"))
            list_processado = []
            list_files_db = []
        except Exception as e:
            self.validar_erro(e, "EXTRAÇÃO")
            return False
        
        try: # TRATAMENTO DOS DADOS
            
            for file in pasta_files:
                NAME_FILE = os.path.basename(file)
                if NAME_FILE in arquivos_processados:
                    list_files_db.append(NAME_FILE)
                    continue
                try:
                    df_TEMPORARIO = pd.read_excel(file, header= 2, sheet_name= "RELATORIO")
                    df_TEMPORARIO = df_TEMPORARIO.loc[df_TEMPORARIO['Retorno'].isin(['CHEIO', 'VAZIO'])]
                    df_TEMPORARIO = df_TEMPORARIO[['END', 'COD', 'DESCRIÇÃO', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'Retorno', 'data_relatorio']]
                    df_TEMPORARIO['NAME_FILE'] = NAME_FILE
                    list_processado.append(df_TEMPORARIO)
                except Exception as e:
                    self.validar_erro(e, f"\n{NAME_FILE} - consolidação")

            df_consolidado = pd.concat(list_processado, axis= 'index')

            list_renomear = {
                'DESCRIÇÃO':'DESCRICAO'
                ,'data_relatorio':'DATA_RELATORIO'
                ,'Retorno':'RETORNO'
                ,'NAME_FILE':'NOME_RELATORIO'
            }
            df_consolidado = df_consolidado.rename(columns=list_renomear)

            col_int = ['END', 'COD','RUA','PREDIO','NIVEL','APTO']
            for col in col_int:
                try:
                    df_consolidado[col] = df_consolidado[col].fillna(0).astype(int)
                except Exception as e:
                    self.validar_erro(e, f"{col} - tratamento_int")

            self.atualizar(df_consolidado, self.NOME_TABELA)
            return True
        except Exception as e:
            self.validar_erro(e, "EXTRAÇÃO")
            return False

if __name__ == '__main__':
    cheio_vazio()
    input("Aperte 'enter' para finalizar o processo...")