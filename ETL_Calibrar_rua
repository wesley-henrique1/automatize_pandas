from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np

# PATH DAS BASES
p_sug = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\Wesley Henrique\CALIBRAÇÃO FILIAL 11\RUA 13 FINI\SUGESTAO.txt'
p_movi = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\Wesley Henrique\CALIBRAÇÃO FILIAL 11\RUA 13 FINI\MOVI.txt'
p_acesso = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\Wesley Henrique\CALIBRAÇÃO FILIAL 11\RUA 13 FINI\acesso.xlsx'
p_prod = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\Wesley Henrique\CALIBRAÇÃO FILIAL 11\RUA 13 FINI\PROD.xlsx'
p_output = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\Wesley Henrique\CALIBRAÇÃO FILIAL 11\RUA 13 FINI\MAPA.xlsx'

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
        df_sug = pd.read_csv(p_sug, header= None, names= col_name.c82)
        df_movi = pd.read_csv(p_movi, header= None, names= col_name.cMovi)
        df_acesso = pd.read_excel(p_acesso, usecols= ['CODPROD', 'QTOS'])
        df_prod = pd.read_excel(p_prod, usecols= ['CODPROD','OBS2', 'QTUNITCX', 'QTTOTPAL'])
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

    try:   
        df_movi = df_movi[['COD', 'MOVI', 'CLASSE']]
        df_geral = df_sug.merge(df_prod, left_on='COD', right_on='CODPROD', how= 'left').drop(columns= 'CODPROD')
        df_geral = df_geral.merge(df_movi, left_on= 'COD', right_on='COD', how= 'left')
        df_geral = df_geral.merge(df_acesso, left_on= 'COD', right_on= 'CODPROD', how= 'left').drop(columns= 'CODPROD')

        df_geral['PL_ATUAL'] = (df_geral['CAP'].astype(int) / df_geral['QTTOTPAL'].astype(int)).round(2)
        df_geral['PL_SUG'] = (df_geral['COM FATOR'].astype(int) / df_geral['QTTOTPAL'].astype(int)).round(2)
        var = df_geral[['COD', 'DESCRIÇÃO', 'CAP','QTTOTPAL', 'PL_ATUAL','PL_SUG']]
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

    with pd.ExcelWriter(p_output) as dest:
        df_geral.to_excel(dest, index= False, sheet_name= 'relatorio')

if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")