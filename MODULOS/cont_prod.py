from modulos._settings import Directory, DB_acumulado
import datetime as dt
import pandas as pd
import glob
import os 

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

class auxiliares:
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
            f"FONTE: cont_prod.py | ETAPA: {etapa} | DATA: {agora}\n"
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
            db_dados = pd.read_sql(text(consulta), self.engine)
            return db_dados
        except Exception as e:
            self.validar_erro(e, "Consulta_banco_dados")
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
            self.validar_erro(e, f"Load_{tabela}")
            return False
class Contagem_INV(auxiliares):
    def __init__(self):
        super().__init__()
        DRIVER = DB_acumulado.drive
        DB_PATH = DB_acumulado.path_acumulado

        self.TABELA_PROD = DB_acumulado.db_prod
        self.TABELA_CONT = DB_acumulado.db_cont

        self.ODBC_CONN_STR = (f"DRIVER={DRIVER};" f"DBQ={DB_PATH};")
        self.list_direct = [Directory.dir_PROD, Directory.dir_CONT]


    def carregamento(self):
        return []
    def pipeline(self):
        try:
            inv_prod = glob.glob(os.path.join(self.list_direct[0], "*.xls*"))
            inv_cont = glob.glob(os.path.join(self.list_direct[1], "*.xls*"))

            db_prod = self.cosultar_db(f"SELECT NOME_ARQ FROM {self.TABELA_PROD}")
            dados_prod = set(db_prod['NOME_ARQ'].str.strip().tolist())

            db_cont =self.cosultar_db(f"SELECT COD_INV FROM {self.TABELA_CONT}")
            dados_cont = set(db_cont['COD_INV'].str.strip().astype(int).tolist())
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False
        
        try:
            try:
                BOOL_PROD = False
                listagem_prod = []
                for file in inv_prod:
                    if file in dados_prod:
                        continue
                    nome_file = os.path.basename(file)
                    try:
                        inv_cod = os.path.splitext(nome_file)
                        codigo_limpo = int((inv_cod[0]).strip())
                        ficante_df = pd.read_excel(file, header= 1, usecols= ["Código", "Descrição", "Rua", "Inventário"])
                    except Exception as e:
                        self.validar_erro(e, nome_file)
                        continue

                    ficante_df = ficante_df.rename(columns={
                        "Código": "COD_PROD"
                        ,"Rua": "RUA"
                        ,"Descrição": "DESC"
                        ,"Inventário": "CONTAGEM"
                    })
                    ficante_df['NOME_ARQ'] = nome_file
                    ficante_df['COD_INV'] = codigo_limpo
                    ficante_df = ficante_df[['COD_INV', "COD_PROD","DESC", "RUA", "CONTAGEM", "NOME_ARQ"]]
                    listagem_prod.append(ficante_df)
                if listagem_prod:
                    df_PROD = pd.concat(listagem_prod, ignore_index=True)
                    BOOL_PROD = True
            except Exception as e:
                self.validar_erro(e, "T-INV_PROD")
            try:
                BOOL_CONT = False
                listagem_cont = []

                for file in inv_cont:
                    try:
                        nome_file = os.path.basename(file)

                        nome_sem_ext = os.path.splitext(nome_file)
                        partes = (nome_sem_ext[0]).split('_')
                        cod_limpo = int((partes[0]).strip())
                        num_contagem = int(partes[1].strip())
                    except Exception as e:
                        self.validar_erro(e, f"T-INV_CONT:{file}")

                    if cod_limpo in dados_cont:
                        continue
                    try:
                        ficante_df = pd.read_excel(file, usecols= ["Contador", "End. OS", "Inicio Cont.", "Fim cont."])

                        cont_func = ficante_df['Contador'].nunique()
                        qt_end = ficante_df['End. OS'].sum()
                        inicio_dt = ficante_df['Inicio Cont.'].min()
                        fim_dt = ficante_df['Fim cont.'].max()
                        df_analitico = pd.DataFrame({
                            "COD_INV": [cod_limpo]
                            ,"QT_FUNC": [cont_func]
                            ,"QT_END": [qt_end]
                            ,"INICIO": [inicio_dt]
                            ,"FIM": [fim_dt]
                            ,"contagem": [num_contagem]
                        })
                        listagem_cont.append(df_analitico)
                    except Exception as e:
                        self.validar_erro(e, nome_file)
                        continue

                if listagem_cont:
                    df_CONT = pd.concat(listagem_cont, ignore_index= True)

                    cont_final = df_CONT.groupby("COD_INV").agg(
                        QT_FUNC = ("QT_FUNC", "nunique")
                        ,QT_END = ("QT_END", "sum")
                        ,INICIO = ("INICIO", "min")
                        ,FIM = ("FIM", "max")
                        ,CONTAGEM = ("contagem", "nunique") 
                    ).reset_index()
                    delta = cont_final["FIM"] - cont_final["INICIO"]
                    cont_final["TEMPO"] = delta.dt.total_seconds() / 60         
                    BOOL_CONT = True
            except Exception as e:
                self.validar_erro(e, "T-INV_CONT")
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        
        try:
            if BOOL_PROD:
                self.atualizar(df_PROD, self.TABELA_PROD)
                print("prod_val")

            if BOOL_CONT:
                self.atualizar(cont_final, self.TABELA_CONT)
                print("pcont_val")

            return True
        except Exception as e:
            self.validar_erro(e, "Laod")
            return False