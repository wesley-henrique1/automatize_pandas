from OUTROS.path_arquivos import *
import pandas as pd
import glob
import os

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
        arquivos = glob.glob(os.path.join(pasta.p_cheio_vazio, "*.xls*"))
        lista = []
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 1: {error}\n")

    try:
        for arquivo in arquivos:
            x = pd.read_excel(arquivo, header= 2)
            lista.append(x)
        df_temp = pd.concat(lista, axis= 0, ignore_index= True)

        df = df_temp.loc[df_temp['Retorno'] != 'SEM RETORNO'].copy()

        df = df.drop(columns= ['Unnamed: 11', 'Unnamed: 12','Unnamed: 13', 'Unnamed: 14'])
        df = df.dropna(subset=['END'])
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 1: {error}\n")

    try:
        df.to_excel(output.cheio_vazio, index=False, sheet_name= "FATO_GERAL")
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 1: {error}\n")

if __name__ == "__main__":
    app = app()
    input("\nPressione Enter para sair...")
