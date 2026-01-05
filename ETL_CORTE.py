import sys
caminho_env = r'C:\Users\wesley.oliveira\WS_OLIVEIRA\SCRIPTS\.meu_ambiente\Lib\site-packages'
if caminho_env not in sys.path:
    sys.path.insert(0, caminho_env)

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from config.config_path import *
from config.fuction import Funcao
import pandas as pd
import warnings
import glob
import os 
warnings.simplefilter(action='ignore', category=UserWarning)


class corte:
    def __init__(self):
        DRIVER = DB_acumulado.drive
        DB_ACUMULADO = DB_acumulado.path_acumulado
        self.NOME_TABELA = DB_acumulado.db_8041
        self.ODBC_CONN_STR = (f"DRIVER={DRIVER};" f"DBQ={DB_ACUMULADO};")

        self.ano = 2026


        self.pipeline()
        input()

    def conectar_db(self, coluna,tabela):
        query_select = f"SELECT {coluna} FROM {tabela}"
        query_tb = f"SELECT * FROM {tabela}"

        try:
            connection_url = URL.create(
            "access+pyodbc",
            query={"odbc_connect":  self.ODBC_CONN_STR}
            )
            self.engine = create_engine(connection_url)
            with self.engine.connect() as connection:
                result = connection.execute(text(query_select))
                dados_existentes = result.fetchall()

                result_1 = connection.execute(text(query_tb))
                tb_dados = result_1.fetchall()
                nomes_colunas = result_1.keys()
                df_tb_dados = pd.DataFrame(
                    tb_dados,
                    columns=nomes_colunas
                )

                print("Conectado com sucesso ao Access!")
                print("DataFrame criado com sucesso!")
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(error)
            
        return dados_existentes, df_tb_dados        
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
    def loop_apre(self, df1, id):
        if   id == 1:
            print(f"\n{"Relatorio corte":-^88}\n")
            print(f"{"DIA":-^88}\n",
                f"{"DATA":<12} {"VL_CORTE":<12} {"QTDE_CORTE":<10} {"QTDE_ITEM":<12} {"DIA":<14} {"MÊS":<14} {"ANO":<4}")
            for x in df1.itertuples():
                print(f"|{x.data:<12} {x.vl_corte:<12} {x.qtde_corte:<10} {x.qtde_item:<12} {x.dia:<14} {x.mes:<14} {x.ano:<4}|")
            print("-" * 88)
            print("\n")
        elif id == 2:
            print(f"{"NOITE":-^88}\n",
                f"{"DATA":<12} {"VL_CORTE":<12} {"QTDE_CORTE":<10} {"QTDE_ITEM":<12} {"DIA":<14} {"MÊS":<14} {"ANO":<4}")
            for x in df1.itertuples():
                print(f"|{x.data_turno:<12} {x.vl_corte:<12} {x.qtde_corte:<10} {x.qtde_item:<12} {x.dia:<14} {x.mes:<14} {x.ano:<4}|")
            print("-" * 88)
            print("\n")
        elif id == 3:
            print(f"{"DIVERGENCIA":-^88}\n",
                f"{"DATA":<14} {"QTDE_PEDIDOS":<14} {"PED_IMPERFEITO":<14} {"DIA":<14} {"MÊS":<14} {"ANO":<4}")
            
            for x in df1.itertuples():
                data_formatada = x.data_turno.strftime('%d-%m-%Y')
                print(f"|{data_formatada:<14} {x.QTDE_PED:<14} {x.ped_imperfeito:<14} {x.dia:<14} {x.mes:<14} {x.ano:<4}|")
            print("-" * 88)
            print("\n")            


    def pipeline(self):
        try: # Extração dos dados nessecario 
            files = glob.glob(os.path.join(directory.dir_41, '*.xls*'))
            names_db, base_cons = self.conectar_db('NOME_ARQUIVO', self.NOME_TABELA)
            arquivos_processados = set([tupla[0].strip() for tupla in names_db if tupla[0] is not None])

            df_corte = pd.read_csv(wms.wms_67,header= None, names= col_names.col_67)
            print(len(arquivos_processados))
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(F"Extração: {error}")
        
        try:
            try:
                df_corte['hora'] = df_corte['hr'].astype(str) + ':' + df_corte['min'].astype(str)
                df_corte['data'] = pd.to_datetime(df_corte['data'], format= '%d/%m/%y').sort_index(axis= 0, ascending= True)
                df_corte['hora'] = pd.to_datetime(df_corte['hora'], format= '%H:%M').dt.strftime('%H:%M')

                col_ajustar = ['vl_corte', 'qtde_corte']
                for col in col_ajustar:
                    df_corte = Funcao.ajustar_numero(df_corte, col, float)
                df_corte = df_corte.sort_values(by="data", ascending= True, axis= 0)
            except Exception as e:
                error = Funcao.validar_erro(e)
                print(F"\nCortes geral: {error}")

            try:
                df_dia = df_corte.loc[df_corte['hora'].between("07:30:00", "18:00:00")].copy()

                var_dia = df_dia.groupby('data').agg(
                            vl_corte=('vl_corte', 'sum'),
                            qtde_corte=('qtde_corte', 'count'),
                            qtde_item=('desc', 'nunique')
                        ).reset_index()
                
                var_dia = Funcao.extrair_e_ordenar_data(var_dia, 'data')
                var_dia['vl_corte'] = var_dia['vl_corte'].round(2).astype(str).str.replace('.', ',', regex= False)
                var_dia = var_dia.sort_values(by= 'data', ascending= True, axis= 0)
            except Exception as e:
                error = Funcao.validar_erro(e)
                print(F"\nCortes dias: {error}")

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
                var_noite = Funcao.extrair_e_ordenar_data(var_noite, 'data_turno')
                var_noite['vl_corte'] = var_noite['vl_corte'].round(2).astype(str).str.replace('.', ',', regex= False)
                var_noite = var_noite.sort_values(by='data_turno', ascending= True, axis= 0)
            except Exception as e:
                error = Funcao.validar_erro(e)
                print(F"\nCorte noite: {error}")

            try:
                list_consolidado = []
                list_processados = []
                dic_erros = {}

                if not files:
                    print("Nenhum arquivo Excel encontrado na pasta especificada.")
                
                for file in files:
                    name_file = os.path.basename(file)
                    if name_file in arquivos_processados:
                        list_consolidado.append(name_file)
                        continue

                    try: # PROCESSAR ARQUIVOS DA PASTA
                        df_var = pd.read_excel(file)
                        if 'NUMPED' not in df_var.columns:
                            dic_erros['PED_COLUMNS'] = str(name_file), "COLUNA 'NUMPED' INEXISTENTE"
                            continue

                        pedido = df_var['NUMPED'].nunique()
                        produto = df_var['PRODUTO'].nunique()

                        df_pedidos = pd.DataFrame({
                            "NOME_ARQUIVO" :  [name_file]
                            ,"QTDE_PED"  : [pedido]
                            ,"QTDE_PROD" : [produto]
                        })
                        list_processados.append(df_pedidos)
                        
                    except Exception as e:
                        error = Funcao.validar_erro(e)
                        dic_erros['PED_PASTA'] = str(name_file), str(error)
                        continue

                if not list_processados:
                    df_cons = base_cons.copy()
                    if 'DATA' in df_cons.columns:
                        df_cons['DATA'] = pd.to_datetime(df_cons['DATA'], format='%d-%m-%Y', errors='coerce')
                
                else:
                    df_novos = pd.concat(list_processados, ignore_index= True)

                    df_novos['DATA_BRUTA'] = df_novos['NOME_ARQUIVO'].str.extract(r'(\d{2}[-_]\d{2})')
                    df_novos['DATA_COMPLETA'] = df_novos['DATA_BRUTA'].str.replace('_', '-') + f"-{self.ano}"
                    df_novos['DATA'] = pd.to_datetime(df_novos['DATA_COMPLETA'], format='%d-%m-%Y')

                    df_novos.drop(columns=['DATA_BRUTA', 'DATA_COMPLETA'], inplace=True)
                    df_novos = df_novos[['DATA','QTDE_PROD', 'QTDE_PED', 'NOME_ARQUIVO']]

                    df_cons = pd.concat([base_cons, df_novos], ignore_index= True).sort_values(by='DATA', ascending= True, axis= 0)
                    df_cons = df_cons.drop_duplicates(subset= None, keep= 'first')
            except Exception as e:
                error = Funcao.validar_erro(e)
                print(F"\nPedidos: {error}")
            
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
                var_div = var_div.merge(df_cons, left_on= 'data_turno', right_on= 'DATA', how= 'left').drop(columns= {'NOME_ARQUIVO', 'QTDE_PROD'}).sort_values(by="data_turno", ascending= True, axis= 0)
            except Exception as e:
                error = Funcao.validar_erro(e)
                print(F"\nDivergencia: {error}")
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(F"\nTratamento: {error}")

        try:
            max_ex_dia= df_dia['data'].max() 
            max_ex_noite = df_noite['data_turno'].max()

            ex_dia = df_dia.loc[df_dia['data'] == max_ex_dia].copy()
            ex_noite = df_noite.loc[df_noite['data_turno'] == max_ex_noite].copy()
            

            with pd.ExcelWriter(output.corte) as destino_corte:
                df_corte.to_excel(destino_corte, sheet_name='extrato', index= False)
                ex_dia.to_excel(destino_corte, sheet_name= 'ex_dia', index= False)
                ex_noite.to_excel(destino_corte, sheet_name= 'ex_noite', index= False)

            if list_processados:
                self.atualizar(df_novos, self.NOME_TABELA)
        except Exception as e:
            error = Funcao.validar_erro(e)
            print(F"Carga: {error}")

        try:
            print("-" * 88)
            self.loop_apre(var_dia, 1)
            self.loop_apre(var_noite, 2)
            self.loop_apre(var_div, 3)

            if not list_consolidado:
                print(f"{len(list_processados)} arquivos consolidados com sucesso.\n")
            elif list_consolidado:
                print("Nenhum novo arquivo foi consolidado nesta execução.\n")

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
            error = Funcao.validar_erro(e)
            print(F"Apresentação: {error}")

if __name__ == '__main__':
    corte()
    input("Aperte 'enter' para finalizar o processo...")