from config_path import Directory, DB_acumulado
import datetime as dt
import pandas as pd
import glob
import os 

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL



class auxiliares:
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
            f"FONTE: corte | ETAPA: {etapa} | DATA: {agora}\n"
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
            self.validar_erro(e, "Consulta_banco_dados")
            return False

class Contagem_inv(auxiliares):
    def __init__(self):
        super().__init__()

        DRIVER = DB_acumulado.drive
        DB_PATH = DB_acumulado.path_acumulado
        self.TABELA_CONT = DB_acumulado.db_contagem
        self.TABELA_TEMP = DB_acumulado.db_tempoINV
        self.ODBC_CONN_STR = (f"DRIVER={DRIVER};" f"DBQ={DB_PATH};")
        self.list_direct = [Directory.dir_div, Directory.dir_temp]
        self.pipeline()
    def pipeline(self):
        try:
            pasta_cont = glob.glob(os.path.join(self.list_direct[0]))
            pasta_temp = glob.glob(os.path.join(self.list_direct[1]))

            triagem_cont = f"SELECT NOME_ARQUIVOS FROM {self.TABELA_CONT}"
            triagem_temp = f"SELECT NOME_ARQUIVO FROM {self.TABELA_TEMP}"
        except Exception as e:
            self.validar_erro(e, "Etração")
            return False
        
        try:
            pass
        except Exception as e:
            self.validar_erro(e, "Tratamento")
            return False
        
        try:
            pass
        except Exception as e:
            self.validar_erro(e, "Carga")
            return False


