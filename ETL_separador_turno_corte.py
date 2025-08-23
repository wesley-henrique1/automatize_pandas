from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np
espaco = '='
inicio = '07:30:00'
fim = '18:00:00'

def app():
    try:
        print('Iniciando o controle de corte, aguarde...')
        rel = pd.read_csv(ar_csv.ar_67, header=None, names= col_name.c67)

        rel['hora'] = rel['hr'].astype(str) + ':' + rel['min'].astype(str)
        rel['data'] = pd.to_datetime(rel['data'], format= '%d/%m/%y')
        rel['hora'] = pd.to_datetime(rel['hora'], format= '%H:%M').dt.strftime('%H:%M')
        rel['vl_corte'] = rel['vl_corte'].astype(str)
        rel['vl_corte'] = rel['vl_corte'].str.replace('.', '', regex=False)
        rel['vl_corte'] = rel['vl_corte'].str.replace(',', '.').astype(float)
        rel['qtde_corte'] = rel['qtde_corte'].astype(str)
        rel['qtde_corte'] = rel['qtde_corte'].str.replace('.', '', regex=False)
        rel['qtde_corte'] = rel['qtde_corte'].str.replace(',', '.').astype(float)

        dia = rel['hora'].between(inicio, fim)
        
        df_dia = pd.DataFrame(rel[dia], columns=['data', 'n_car', 'cod','desc', 'n_ped', "qtde_orig", 'vl_orig', 'rua', 'predio', 'apto', 'estoque_dia', 'qtde_corte', 'vl_corte', 'hr', 'min', 'motivo', 'cod_fuc','func', 'hora'])
        df_dia['data'] = pd.to_datetime(df_dia['data'], format= '%d/%m/%Y')
        var_dia = df_dia.groupby('data').agg(
            vl_corte=('vl_corte', 'sum'),
            qtde_corte=('qtde_corte', 'count'),
            qtde_item=('desc', 'nunique')
        ).reset_index()
        
        var_dia['dia'] = var_dia['data'].dt.day_name('pt_BR')
        var_dia['mes'] = var_dia['data'].dt.month_name('pt_BR')
        var_dia['ano'] = var_dia['data'].dt.year
        max_ex_dia= df_dia['data'].max()
        ex_dia = df_dia.loc[df_dia['data'] == max_ex_dia]
        var_dia['data'] = var_dia['data'].dt.strftime("%d-%m-%Y")

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

        var_dia['vl_corte'] = var_dia['vl_corte'].apply(str).str.replace('.', ',', regex= False)
        var_noite['vl_corte'] = var_noite['vl_corte'].apply(str).str.replace('.', ',', regex= False)
  
        print(f"=" * 70)
        print(f"Relatorio de corte:")
        print(f"DIA:\n", var_dia)
        print(f"=" * 70)
        print(f"NOITE:\n",var_noite)
        print(f"=" * 70)
        print(f"DIVERGENCIA:\n", var_div)
        rel.drop(columns=['hora',], inplace= True)
        try:
            with pd.ExcelWriter(output.corte) as writer:
                rel.to_excel(writer, sheet_name='extrato', index= False)
                ex_dia.to_excel(writer, sheet_name= 'ex_dia', index= False)
                ex_noite.to_excel(writer, sheet_name= 'ex_noite', index= False)
                print('fim do processo, verifique o arquivo controle_corte.xlsx')

        except PermissionError as e:
            print(espaco * 100)
            print(f"arquivo em execução: {e}")
            print(espaco * 100)
            print('Arquivo em execução, feche o arquivo controle_corte.xlsx e tente novamente.')
    except FileNotFoundError as e:
        print(espaco * 100)
        print(f"Arquivo não encontrado: {e}")
        print(espaco * 100)
        print('Arquivo Report.txt não encontrado, verifique o caminho e tente novamente.')
    except Exception as e:
        print(espaco * 100)
        print(f"Erro inesperado: {e}")
    except PermissionError as e:
        print(espaco * 100)
        print(f"arquivo em execução: {e}")
        print(espaco * 100)
        print('Arquivo em execução, feche o arquivo controle_corte.xlsx e tente novamente.')


if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")