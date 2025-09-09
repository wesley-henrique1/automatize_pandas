from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np
import warnings
import glob
import os
warnings.simplefilter(action='ignore', category=UserWarning)

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
def app():

    try:
        arquivos_excel = glob.glob(os.path.join(pasta.p_41, '*9.xls*'))
        total_arquivos = len(arquivos_excel)

        if total_arquivos == 0:
            print("Nenhum arquivo Excel encontrado na pasta especificada.")
            return
        print("Iniciando contagem de pedidos, aguarde...")
        for i, arquivo in enumerate(arquivos_excel, 1):
            nome_arquivo = os.path.basename(arquivo)
            try:
                df = pd.read_excel(arquivo)
                
                # Verifica colunas necessárias
                if 'NUMPED' not in df.columns:
                    print("   AVISO: Coluna 'NUMPED' não encontrada\n")
                    continue
                                
                cont_nunique = df['NUMPED'].nunique()
                print(f"{nome_arquivo}: {cont_nunique}")
            except Exception as e:
                error = validar_erro(e)
                print(error)
                continue
        print(f"total arquivos: {i}")
    except Exception as e:
        error = validar_erro(e)
        print(F"ETAPA_PEDIDO: {error}")
        exit()

    try:
        print('Iniciando o controle de corte, aguarde...')
        rel = pd.read_csv(ar_csv.ar_67, header=None, names= col_name.c67)

        rel['hora'] = rel['hr'].astype(str) + ':' + rel['min'].astype(str)
        rel['data'] = pd.to_datetime(rel['data'], format= '%d/%m/%y').sort_index(axis= 0, ascending= True)
        rel['hora'] = pd.to_datetime(rel['hora'], format= '%H:%M').dt.strftime('%H:%M')
        rel['vl_corte'] = rel['vl_corte'].astype(str)
        rel['vl_corte'] = rel['vl_corte'].str.replace('.', '', regex=False)
        rel['vl_corte'] = rel['vl_corte'].str.replace(',', '.').astype(float)
        rel['qtde_corte'] = rel['qtde_corte'].astype(str)
        rel['qtde_corte'] = rel['qtde_corte'].str.replace('.', '', regex=False)
        rel['qtde_corte'] = rel['qtde_corte'].str.replace(',', '.').astype(float)

        inicio = '07:30:00'
        fim = '18:00:00'
        dia = rel['hora'].between(inicio, fim)
        
        df_dia = pd.DataFrame(rel[dia], columns=['data', 'n_car', 'cod','desc', 'n_ped', "qtde_orig", 'vl_orig', 'rua', 'predio', 'apto', 'estoque_dia', 'qtde_corte', 'vl_corte', 'hr', 'min', 'motivo', 'cod_fuc','func', 'hora'])
        df_dia['data'] = pd.to_datetime(df_dia['data'], format= '%d/%m/%Y')
        var_dia = df_dia.groupby('data').agg(
            vl_corte=('vl_corte', 'sum'),
            qtde_corte=('qtde_corte', 'count'),
            qtde_item=('desc', 'nunique')
        ).reset_index()
    except Exception as e:
        error = validar_erro(e)
        print(F"ETAPA_1: {error}")
        exit()
    try:
        var_dia['dia'] = var_dia['data'].dt.day_name('pt_BR')
        var_dia['mes'] = var_dia['data'].dt.month_name('pt_BR')
        var_dia['ano'] = var_dia['data'].dt.year
        max_ex_dia= df_dia['data'].max()
        ex_dia = df_dia.loc[df_dia['data'] == max_ex_dia]
        var_dia['data'] = var_dia['data'].dt.strftime("%d-%m-%Y")
        var_dia['vl_corte'] = var_dia['vl_corte'].round(2).astype(str).str.replace('.', ',', regex= False)

        print(f"=" * 70)
        print(f"Relatorio de corte:")
        print(f"DIA:\n", var_dia)
    except Exception as e:
        error = validar_erro(e)
        print(F"ETAPA_DIA: {error}")
        exit()

    try:
        df_noite = pd.DataFrame(rel[~dia], columns=['data', 'n_car', 'cod','desc', 'n_ped', "qtde_orig", 'vl_orig', 'rua', 'predio', 'apto', 'estoque_dia', 'qtde_corte', 'vl_corte', 'hr', 'min', 'motivo', 'cod_fuc','func', 'hora'])
        df_noite['data'] = pd.to_datetime(df_noite['data'], format= '%d/%m/%Y')
        df_noite["data_turno"] = df_noite['data'].copy()
        df_noite.loc[df_noite['hora'] < inicio, 'data_turno'] -= pd.Timedelta(days=1)
        var_noite = df_noite.groupby('data_turno').agg(
            vl_corte=('vl_corte', 'sum'),
            qtde_corte=('qtde_corte', 'count'),
            qtde_item=('desc', 'nunique')
        ).reset_index()
        var_noite['dia'] = var_noite['data_turno'].dt.day_name('pt_BR')
        var_noite['mes'] = var_noite['data_turno'].dt.month_name('pt_BR')
        var_noite['ano'] = var_noite['data_turno'].dt.year
        max_ex_noite = df_noite['data_turno'].max()
        var_noite['data_turno'] = var_noite['data_turno'].dt.strftime("%d-%m-%Y")     
        ex_noite = df_noite.loc[df_noite['data_turno'] == max_ex_noite]
        var_noite['vl_corte'] = var_noite['vl_corte'].round(2).astype(str).str.replace('.', ',', regex= False)

        print(f"=" * 70)
        print(f"NOITE:\n",var_noite)
    except Exception as e:
        error = validar_erro(e)
        print(F"ETAPA_NOITE: {error}")
        exit()

    try:
        df_div = pd.DataFrame(rel, columns=['data','desc','hora', 'n_ped'])
        df_div['data'] = pd.to_datetime(df_div['data'], format= '%d/%m/%Y')
        df_div["data_turno"] = df_div['data'].copy()
        df_div.loc[df_div['hora'] < inicio, 'data_turno'] -= pd.Timedelta(days=1)
        var_div = df_div.groupby('data_turno').agg(
            ped_imperfeito=('n_ped', 'nunique')
        ).reset_index()
        var_div['dia'] = var_div['data_turno'].dt.day_name('pt_BR')
        var_div['mes'] = var_div['data_turno'].dt.month_name('pt_BR')
        var_div['ano'] = var_div['data_turno'].dt.year
        var_div['data_turno'] = var_div['data_turno'].dt.strftime("%d-%m-%Y")
        print(f"=" * 70)
        print(f"DIVERGENCIA:\n", var_div)
    except Exception as e:
        error = validar_erro(e)
        print(F"ETAPA_DIVER: {error}")
        exit()

    try:
        # var_dia['vl_corte'] = var_dia['vl_corte'].astype(str).str.replace('.', ',', regex= False)
        # var_noite['vl_corte'] = var_noite['vl_corte'].astype(str).str.replace('.', ',', regex= False)
        rel.drop(columns=['hora',], inplace= True)

        with pd.ExcelWriter(output.corte) as writer:
            rel.to_excel(writer, sheet_name='extrato', index= False)
            ex_dia.to_excel(writer, sheet_name= 'ex_dia', index= False)
            ex_noite.to_excel(writer, sheet_name= 'ex_noite', index= False)
            print('fim do processo, verifique o arquivo controle_corte.xlsx')
    except Exception as e:
        error = validar_erro(e)
        print(F"ETAPA_FINAL: {error}")
        exit()

if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")