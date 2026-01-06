from MODULOS.config_path import *
import pandas as pd
import numpy as np
import glob
import os

class auxiliar:
    def validar_erro(e):
        print("=" * 60)
        if isinstance(e, KeyError):
            return f"KeyError: A coluna ou chave '{e}' não foi encontrada."
        elif isinstance(e, PermissionError):
            return "PermissionError: O arquivo está sendo usado ou você não tem permissão para acessá-lo. Por favor, feche o arquivo."
        elif isinstance(e, TypeError):
            return f"TypeError: Erro de tipo. Verifique se os dados são do tipo correto. Mensagem original: {e}"
        elif isinstance(e, ValueError):
            return f"ValueError: Erro de valor. Mensagem original: {e}"
        else:
            return f"Ocorreu um erro inesperado: {e}"
    def loop_df(dir, heade,acum):
        list_pular = []
        list_temporaria = []
        
        for files in dir:
            nome_arquivo = os.path.basename(files)      
            delimitador_1 = os.path.splitext(nome_arquivo)[0]
            
            if nome_arquivo in acum['NAME_FILES']:
                print(nome_arquivo)
                list_pular.append(nome_arquivo)
                continue

            df_temporariro = pd.read_excel(files, header= heade)
            delimitador_11 = delimitador_1.split("_")[0]
            df_temporariro['INV'] = int(delimitador_11)

            if "_" in delimitador_1:
                delimitador_12 = delimitador_1.split("_")[1]
                df_temporariro['CONTAGEM'] = int(delimitador_12)
                
            df_temporariro['NAME_FILES'] = nome_arquivo
            if "Unnamed: 13" in df_temporariro.columns:
                df_temporariro = df_temporariro.drop(columns= "Unnamed: 13")
            
            list_temporaria.append(df_temporariro)

        if not list_temporaria:
            print(list_pular)
            return None
        if list_pular:   
            print(list_pular)   
            
        df_final = pd.concat(list_temporaria, ignore_index= False)
        return df_final
class consolidado_inv(auxiliar):
    print(F"\n{"ANALISE DE INVENTARIO":_^78}\n")
    print("=" * 78)

    try:
        df_contagem = pd.read_excel(power_bi.acu_inv, sheet_name= 'ACUMULADO')
        df_divergencia = pd.read_excel(power_bi.acu_inv, sheet_name= 'DIVERGENCIA')

        dir_div = glob.glob(os.path.join(directory.dir_div, "*.xls*"))
        dir_temp = glob.glob(os.path.join(directory.dir_temp, "*.xls*"))

        df_div = loop_df(dir_div, 1, df_divergencia)
        df_temp = loop_df(dir_temp, 0, df_contagem)
    except Exception as e:
        error = validar_erro(e)
        print(f"Extração: {error}\n")

    try:
        novas_colunas = df_temp['Contador'].str.split(' - ', expand= True)
        novas_colunas.columns =['COD_FUNC', 'FUNC']

        df_temp['COD_FUNC'] = novas_colunas['COD_FUNC'].astype(int)
        df_temp['FUNC'] = novas_colunas['FUNC']
        df_temp['DATA'] = pd.to_datetime(df_temp['Inicio Cont.'], format= '%d-%m-%Y').dt.normalize()
        df_temp['HORAS'] = pd.to_datetime(df_temp['Inicio Cont.'], format='%d-%m-%Y %H:%M:%S').dt.time
        df_temp['tempo_gasto'] = df_temp['Fim cont.'] - df_temp['Inicio Cont.']

        df_fim = df_temp.groupby(['DATA']).agg(
            QTDE_INV = ('INV', 'nunique'),
            END_CONTADO = ('End. contados', 'sum'),
            TEMPO_GASTO = ('tempo_gasto', 'sum'),
            INICIO = ('HORAS', 'min'),
            FIM = ('HORAS', 'max')
        ).reset_index().sort_values(by='DATA', ascending= True)

        temp_nunique = df_temp[['INV']].drop_duplicates().copy()
        div_nunique = df_div[['INV']].drop_duplicates().copy()

        caso_1 = temp_nunique.merge(div_nunique, left_on= 'INV', right_on= 'INV', how= 'left', indicator= True)
        ids_caso_1 = caso_1[caso_1['_merge'] == 'left_only']['INV']
        lista_1 = list(ids_caso_1)

        caso_2 = temp_nunique.merge(div_nunique, left_on= 'INV', right_on= 'INV', how= 'right', indicator= True)
        ids_caso_2 = caso_2[caso_2['_merge'] == 'right_only']['INV']
        lista_2 = list(ids_caso_2)

        if lista_1 and lista_2:
            print("INCONSISTÊNCIA NA CONSOLIDAÇÃO")
            print(f"{f"FALTA DE DIVERGÊNCIA:{lista_1}":^78} ")
            print("\n")
            print(f"{f"FALTA DE CONTAGEM FALTANDO: {lista_2}":^78}\n")

        elif lista_1:
            print(f"{"INCONSISTÊNCIA NA CONSOLIDAÇÃO":^78}")
            print(f"{f"FALTA DE DIVERGÊNCIA:{lista_1}":^78} ")

        elif lista_2:
            print(f"{"INCONSISTÊNCIA NA CONSOLIDAÇÃO":^78}")
            print(f"{f"FALTA DE CONTAGEM FALTANDO: {lista_2}":^78}\n") 

        elif not lista_1 and not lista_2:
            lista_inv = caso_1['INV'].to_list()
            print(f"{"CONSOLIDAÇÃO CONCLUIDA...":^78}")
    except Exception as e:
        error = validar_erro(e)
        print(f"Tratamento: {error}\n")

    try:
        df_temp = df_temp[['COD_FUNC','FUNC', 'End. contados', 'Inicio Cont.', 'Fim cont.', 'INV', 'CONTAGEM', 'tempo_gasto', 'NAME_FILES']]

        with pd.ExcelWriter(power_bi.acu_inv) as destino:
            df_div.to_excel(destino, index= False, sheet_name= "DIVERGENCIA"),
            df_temp.to_excel(destino, index= False, sheet_name= "ACUMULADO"),
        
        df_fim.to_excel(output.inv, index= False, sheet_name= "INVENTARIO")   
    except Exception as e:
        error = validar_erro(e)
        print(f"Carga: {error}\n")

if __name__ in "__main__":
    consolidado_inv()
    input("\n\n* * * Processo Concluído. Pressione ENTER para fechar a aplicação * * *\n")