from config.config_path import *
import pandas as pd
import numpy as np

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
        print("=" * 60)
        print("Processo iniciado, favor aguarde...\n")
        col_96 = ['CODPROD', 'DESCRICAO','OBS2', 'RUA', 'PREDIO', 'APTO', ]
        col_86 = ['Código', 'Estoque', 'Qtde Pedida', 'Bloqueado(Qt.Bloq.-Qt.Avaria)', 'Qt.Avaria', 'Custo real', 'Disponível']
        df_8596 = pd.read_excel(relatorios.rel_96, usecols= col_96)
        df_286 = pd.read_excel(outros.ou_86, usecols= col_86)
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()
        
    try:
        df = df_8596.merge(df_286, left_on= 'CODPROD', right_on= 'Código', how= 'inner')
        df = df[df['RUA'].between(1, 40)]
        df['BLOQ + AV'] = df['Bloqueado(Qt.Bloq.-Qt.Avaria)'] + df['Qt.Avaria']
        df['BLOQ?'] = np.where(df['BLOQ + AV'] >= df['Estoque'], 'S', 'N')
        df['total_custo'] = df['Custo real'] * df['Disponível']
        df.drop(columns=["Código", "Bloqueado(Qt.Bloq.-Qt.Avaria)", "Qt.Avaria"])
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

    try:
        df_os_esquecidos = df.loc[(df['OBS2'] == 'FL') & (df['Estoque'] > 0) & (df['BLOQ?'] == "N")]
        df_fantasminha = df.loc[(df['OBS2'] == 'FL') & (df['Estoque'] == 0)]
        df_hora_extra = df.loc[(df['OBS2'] != 'FL') & (df['Estoque'] == 0) & (df['Qtde Pedida'] == 0)]
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

    try:
        with pd.ExcelWriter(output.controle_fl) as feijao:
            df_os_esquecidos.to_excel(feijao, sheet_name= "FL_ATIVOS", index= False)
            df_fantasminha.to_excel(feijao, sheet_name= "FL_END", index= False)
            df_hora_extra.to_excel(feijao, sheet_name= "INATIVOS", index= False)
            df.to_excel(feijao, sheet_name= "GERAL", index= False)
            print("=" * 60)
            print('fim do processo, verifique o arquivo controle_FL.xlsx')
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")