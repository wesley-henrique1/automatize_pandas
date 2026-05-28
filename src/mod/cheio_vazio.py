from ..lib.settings import BaseDados, Gestao
from ..lib import ValidarErros, MonitorETL

import pandas as pd
import glob
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

class auxiliar:
    def cosultar_db(self, consulta):
        try:
            connection_url = URL.create(
            "access+pyodbc",
            query={"odbc_connect":  self.ODBC_CONN_STR}
            )
            self.engine = create_engine(connection_url)
            db_dados = pd.read_sql(text(consulta), self.engine)
            return db_dados
        except Exception as e:
            self.validador.registrar_log(e, "Consulta_banco_dados")
            return pd.DataFrame()
    def atualizar(self, df, tabela):
        try:            
            df.to_sql(
                name= tabela
                ,con= self.engine
                ,if_exists='append'
                ,index=False
                ,chunksize=100
            )
        except Exception as e:
            self.validador.registrar_log(e, f"Load_{tabela}")
            return False
class CheioVazio(auxiliar):
    validador = ValidarErros(fonte="CheioVazio")
    def __init__(self):
        DRIVER = BaseDados.drive
        DB_PATH = BaseDados.path_acumulado
        self.NOME_TABELA = BaseDados.DBVzCh
        self.ODBC_CONN_STR = (f"DRIVER={DRIVER};" f"DBQ={DB_PATH };")
        self.list_dados = [Gestao.CheioVazio]

        self.Instancia = MonitorETL()

    def pipeline(self):
        try:  # EXTRAÇÃO DOS DADOS
            self.Instancia.stageTime('Extract')
            names_db = self.cosultar_db(
                f"SELECT {'NOME_RELATORIO'} FROM {self.NOME_TABELA}"
            )
            arquivos_processados = set(names_db['NOME_RELATORIO'].str.strip().tolist())

            pasta_files = glob.glob(os.path.join(self.list_dados[0], "*xls*"))
            list_processado = []
            list_files_db = []
            list_erros = []
            self.Instancia.stageTime('Extract')
        except Exception as e:
            self.validador.registrar_log(e, "Extract")
            return False
        try: # TRATAMENTO DOS DADOS
            self.Instancia.stageTime('Transform')
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
                    list_erros.append(NAME_FILE)
                    self.validador.registrar_log(e, f"\n{NAME_FILE} - consolidação")
            if not list_processado:
                self.qt_files = len(list_processado)
                self.qt_erros = len(list_erros)
                self.qtde = len(list_files_db)
                return True
            
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
                    self.validador.registrar_log(e, f"{col} - tratamento_int")
            self.Instancia.stageTime('Transform')
        except Exception as e:
            self.validador.registrar_log(e, "Transform")
            return False
        try:
            self.Instancia.stageTime('Load')
            self.atualizar(df_consolidado, self.NOME_TABELA)
            self.qt_files = len(list_processado)
            self.qt_erros = len(list_erros)
            self.qtde = len(list_files_db)

            self.Instancia.stageTime('Load')
            self.Instancia.conversor(Modulo= "CheioVazio")
            return True
        except Exception as e:
            self.validador.registrar_log(e, "Load")
            return False
    def carregamento(self, validar):
        lista_de_logs = []
        dic_retorno = []
        try:           
            if not validar:
                return
            dic_retorno ={
                "MODULO": "ch_vz"
                ,"ARQUIVOS": self.qt_files
                ,"ERROS": self.qt_erros
                ,"LEITURA": self.qtde 
            }
            return lista_de_logs, dic_retorno
        except Exception as e:
            self.validador.registrar_log(e, "CARREGAMENTO")
            return False
    def outputLog(self, validar):
        ListaOutPut = []
        try:
            if not validar:
                return
            return ListaOutPut
        except Exception as e:
            self.validador.registrar_log(e, "output")
            return False
