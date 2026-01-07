import sys
caminho_env = r'C:\Users\wesley.oliveira\WS_OLIVEIRA\SCRIPTS\.meu_ambiente\Lib\site-packages'
if caminho_env not in sys.path:
    sys.path.insert(0, caminho_env)

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from config_path import *
import datetime as dt
import pandas as pd
import warnings
import glob
import re
import os 
warnings.simplefilter(action='ignore', category=UserWarning)

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
            f"FONTE: main.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )

        try:
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
                print("Conectado com sucesso ao Access!")
                print("DataFrame criado com sucesso!")
        except Exception as e:
            self.validar_erro(e, "Consulta_banco_dados")
            return False

        return db_dados        
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
            self.validar_erro(e, "Consulta_banco_dados")
            return False
            exit()
    def loop_apre_texto(self, df1, id):
        texto_final = ""
        
        # ID 1 e 2: Relatório de Corte (DIA e NOITE)
        if id == 1 or id == 2:
            titulo_turno = "DIA" if id == 1 else "NOITE"
            
            texto_final += f"{'Relatorio corte':-^88}\n"
            texto_final += f"{titulo_turno:-^88}\n\n"
            texto_final += (f"{'DATA':<12} {'VL_CORTE':<12} {'QTDE_CORTE':<10} "
                            f"{'QTDE_ITEM':<12} {'DIA':<14} {'MÊS':<14} {'ANO':<4}\n")
            texto_final += "-" * 88 + "\n"

            for x in df1.itertuples():
                # Verifica qual coluna de data usar baseada no ID
                data_valor = x.data if id == 1 else x.data_turno
                
                # Formata a data se for um objeto datetime
                if hasattr(data_valor, 'strftime'):
                    data_valor = data_valor.strftime('%d-%m-%Y')
                
                texto_final += (f"|{str(data_valor):<12} {str(x.vl_corte):<12} {str(x.qtde_corte):<10} "
                                f"{str(x.qtde_item):<12} {str(x.dia):<14} {str(x.mes):<14} {str(x.ano):<4}|\n")
            
            texto_final += "-" * 88 + "\n"

        # ID 3: Relatório de Divergência
        elif id == 3:
            texto_final += f"{'DIVERGENCIA':-^88}\n\n"
            texto_final += (f"{'DATA':<14} {'QTDE_PEDIDOS':<14} {'PED_IMPERFEITO':<14} "
                            f"{'DIA':<14} {'MÊS':<14} {'ANO':<4}\n")
            texto_final += "-" * 88 + "\n"
            
            for x in df1.itertuples():
                # Formatação segura da data
                data_f = x.data_turno
                if hasattr(data_f, 'strftime'):
                    data_f = data_f.strftime('%d-%m-%Y')
                
                texto_final += (f"|{str(data_f):<14} {str(x.QTDE_PED):<14} {str(x.ped_imperfeito):<14} "
                                f"{str(x.dia):<14} {str(x.mes):<14} {str(x.ano):<4}|\n")
            
            texto_final += "-" * 88 + "\n"

        return texto_final
    def leitura_41(self, ano, caminho):
        pasta_files = glob.glob(os.path.join(caminho, "*.xlsx"))

        list_processados = []
        dic_erros = {}

        for file in pasta_files:
            names = os.path.basename(file)
            df_var = pd.read_excel(file)
            if 'NUMPED' not in df_var.columns:
                dic_erros['PED_COLUMNS'] = str(names), "COLUNA 'NUMPED' INEXISTENTE"
                continue

            pedido = df_var['NUMPED'].nunique()
            produto = df_var['PRODUTO'].nunique()

            data_extraida = re.search(r'(\d{2}[-_]\d{2})', names).group(1)
            data_arrumada = data_extraida.replace('_', '-') + f"-{ano}"
            data_final = pd.to_datetime(data_arrumada, format='%d-%m-%Y')

            df_pedidos = pd.DataFrame({
                "NOME_ARQUIVO" : [names],
                "QTDE_PED"     : [pedido],
                "QTDE_PROD"    : [produto],
                "DATA"         : [data_final]
            })
            list_processados.append(df_pedidos)
    def extrair_e_ordenar_data(df, data):
        var = df.copy()

        var['dia'] = var[data].dt.day_name('pt_BR')
        var['mes'] = var[data].dt.month_name('pt_BR')
        var['ano'] = var[data].dt.year
        var[data] = var[data].dt.strftime("%d-%m-%Y")    
        var = var.sort_values(by= data, ascending= True, axis= 0)
        return var

class corte(auxiliar):
    def __init__(self):
        DRIVER = DB_acumulado.drive
        DB_ACUMULADO = DB_acumulado.path_acumulado
        self.NOME_TABELA = DB_acumulado.db_8041
        self.ODBC_CONN_STR = (f"DRIVER={DRIVER};" f"DBQ={DB_ACUMULADO};")

        self.list_path = [Wms.wms_67]
        self.ano = dt.datetime.now().year

        self.carregamento()
        self.pipeline()


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
            print(lista_de_logs)
            return lista_de_logs
        except Exception as e:
            self.validar_erro(e, "CARREGAMENTO")
            return False
    def pipeline(self):
        try: # Extração dos dados nessecario 
            files_pedidos = glob.glob(os.path.join(Directory.dir_41, '*.xls*'))
            query_select = f"SELECT {'NOME_ARQUIVO'} FROM {self.NOME_TABELA}"
            names_db = list(self.cosultar_db(query_select))
            arquivos_processados = set(names_db['NOME_ARQUIVO'].str.strip().tolist())

            df_corte = pd.read_csv(self.list_path[0],header= None, names= ColNames.col_67)
        except Exception as e:
            self.validar_erro(e, "EXTRAIR")
            return False
        
        try:
            try:
                df_corte['hora'] = df_corte['hr'].astype(str) + ':' + df_corte['min'].astype(str)
                df_corte['data'] = pd.to_datetime(df_corte['data'], format= '%d/%m/%y').sort_index(axis= 0, ascending= True)
                df_corte['hora'] = pd.to_datetime(df_corte['hora'], format= '%H:%M').dt.strftime('%H:%M')

                col_ajustar = ['vl_corte', 'qtde_corte']
                for col in col_ajustar:
                    df_corte = self.ajustar_numero(df_corte, col, float)
                df_corte = df_corte.sort_values(by="data", ascending= True, axis= 0)
            except Exception as e:
                self.validar_erro(e, "CORTE_GERAL")
                return False
            try:
                df_dia = df_corte.loc[df_corte['hora'].between("07:30:00", "18:00:00")].copy()

                var_dia = df_dia.groupby('data').agg(
                            vl_corte=('vl_corte', 'sum'),
                            qtde_corte=('qtde_corte', 'count'),
                            qtde_item=('desc', 'nunique')
                        ).reset_index()
                
                var_dia = self.extrair_e_ordenar_data(var_dia, 'data')
                var_dia['vl_corte'] = var_dia['vl_corte'].round(2).astype(str).str.replace('.', ',', regex= False)
                var_dia = var_dia.sort_values(by= 'data', ascending= True, axis= 0)
            except Exception as e:
                self.validar_erro(e, "CORTE_DIA")
                return False

            try:
                df_noite = df_corte.loc[~df_corte['hora'].between("07:30:00", "18:00:00")].copy()

                df_noite['data'] = pd.to_datetime(df_noite['data'], format= '%d/%m/%Y')
                df_noite["data_turno"] = df_noite['data'].copy()
                df_noite.loc[df_noite['hora'] < "07:30:00", 'data_turno'] -= pd.Timedelta(days=1)
                var_noite = df_noite.groupby('data_turno').agg(
                    vl_corte=('vl_corte', 'sum'),
                    qtde_corte=('qtde_corte', 'count'),
                    qtde_item=('desc', 'nunique')
                ).reset_index()
                var_noite = self.extrair_e_ordenar_data(var_noite, 'data_turno')
                var_noite['vl_corte'] = var_noite['vl_corte'].round(2).astype(str).str.replace('.', ',', regex= False)
                var_noite = var_noite.sort_values(by='data_turno', ascending= True, axis= 0)
            except Exception as e:
                self.validar_erro(e, "CORTE_NOITE")
                return False

            try:
                list_achados = []
                lis_procurados = []
                dic_erros = {}

                if not files_pedidos:
                    aviso = "Nenhum arquivo Excel encontrado na pasta especificada."
                    
                for file in files_pedidos:
                    name_file = os.path.basename(file)
                    if name_file in arquivos_processados:
                        list_achados.append(name_file)
                        continue

                    df_var = pd.read_excel(file)
                    if 'NUMPED' not in df_var.columns:
                        dic_erros['name_file'] = "COLUNA 'NUMPED' INEXISTENTE"
                        continue

                    pedido = df_var['NUMPED'].nunique()
                    produto = df_var['PRODUTO'].nunique()

                    data_extraida = re.search(r'(\d{2}[-_]\d{2})', name_file).group(1)
                    data_arrumada = data_extraida.replace('_', '-') + f"-{self.ano}"
                    data_final = pd.to_datetime(data_arrumada, format='%d-%m-%Y')
                    novo_nome_arquivo = f"PEDIDOS_{data_arrumada}.xlsx"

                    df_pedidos = pd.DataFrame({
                        "NOME_ARQUIVO" : [novo_nome_arquivo],
                        "QTDE_PED"     : [pedido],
                        "QTDE_PROD"    : [produto],
                        "DATA"         : [data_final]
                    })
                    lis_procurados.append(df_pedidos)
                    try:
                        novo_caminho_completo = os.path.join(Directory.dir_41, novo_nome_arquivo)
                        os.rename(file, novo_caminho_completo)
                        print(f"Sucesso: {name_file} -> {novo_nome_arquivo}")
                    except Exception as e:
                        print(f"Erro ao renomear {name_file}: Verifique se o arquivo está aberto!")        
                if lis_procurados:
                    df_final = pd.concat(lis_procurados, ignore_index=True)
                    self.atualizar(df_final, self.NOME_TABELA)
                    query_dados = f"SELECT {'*'} FROM {self.NOME_TABELA}"

                    db_dados = self.cosultar_db(query_dados)
                    

                mensagem_ped = (
                    f"Total de arquivos na pasta: {len(files_pedidos)}\n"
                    ,f"Total de arquivos tratados: {len(lis_procurados)}\n"
                )                                
            except Exception as e:
                self.validar_erro(e, "PEDIDOS")
                return False
            
            try:
                df_div = df_corte[['data','desc','hora', 'n_ped']].copy()
                df_div['data'] = pd.to_datetime(df_div['data'], format= '%d/%m/%Y')
                df_div["data_turno"] = df_div['data'].copy()
                df_div.loc[df_div['hora'] < "07:30:00", 'data_turno'] -= pd.Timedelta(days=1)
                var_div = df_div.groupby('data_turno').agg(
                    ped_imperfeito=('n_ped', 'nunique')
                ).reset_index()
                var_div['dia'] = var_div['data_turno'].dt.day_name('pt_BR')
                var_div['mes'] = var_div['data_turno'].dt.month_name('pt_BR')
                var_div['ano'] = var_div['data_turno'].dt.year
                var_div['data_turno'] = pd.to_datetime(var_div['data_turno'], format='%d-%m-%Y')
                var_div = var_div.merge(db_dados, left_on= 'data_turno', right_on= 'DATA', how= 'left').drop(columns= {'NOME_ARQUIVO', 'QTDE_PROD'}).sort_values(by="data_turno", ascending= True, axis= 0)
            except Exception as e:
                self.validar_erro(e, "DIVERGENCIAS")
                return False
        except Exception as e:
            self.validar_erro(e, "TRATAMENTO")
            return False

        try:
            max_ex_dia= df_dia['data'].max() 
            max_ex_noite = df_noite['data_turno'].max()

            ex_dia = df_dia.loc[df_dia['data'] == max_ex_dia].copy()
            ex_noite = df_noite.loc[df_noite['data_turno'] == max_ex_noite].copy()
            

            with pd.ExcelWriter(Output.corte) as destino_corte:
                df_corte.to_excel(destino_corte, sheet_name='extrato', index= False)
                ex_dia.to_excel(destino_corte, sheet_name= 'ex_dia', index= False)
                ex_noite.to_excel(destino_corte, sheet_name= 'ex_noite', index= False)

        except Exception as e:
                self.validar_erro(e, "CARGA")
                return False

        try:
            print("-" * 88)
            self.loop_apre(var_dia, 1)
            self.loop_apre(var_noite, 2)
            self.loop_apre(var_div, 3)
            LARGURA = 88

            if dic_erros:
                print(f"{'RELATÓRIO DE FALHAS':^{LARGURA}}")
                print("-" * LARGURA)
                for etapa, detalhe_erro in dic_erros.items():
                    nome_arquivo = detalhe_erro[0]
                    descricao_erro = detalhe_erro[1]
                    print(f"ETAPA DA FALHA: {etapa}")
                    print(f"  > Arquivo Afetado: {nome_arquivo}")
                    print(f"  > Causa: {descricao_erro}")
                    print("-" * LARGURA)
            print("-" * 88)    
        except Exception as e:
            error = self.validar_erro(e)
            print(F"Apresentação: {error}")

if __name__ == '__main__':
    corte()
    input("Aperte 'enter' para finalizar o processo...")