from config.config_path import *
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
        print(f"{"CHEIO X VAZIO":_^36}")
        arquivos = glob.glob(os.path.join(directory.dir_cheio_vazio, "*.xls*"))
        lista_erros = []
        lista = []
    except Exception as e:
        error = validar_erro(e)
        print(f"Extração: {error}\n")

    try:
        for arquivo in arquivos:
            try:
                names_files = os.path.basename(arquivo)
                x = pd.read_excel(arquivo, header= 2)
                x['NAMES_FILES'] = names_files
                lista.append(x)
            except Exception as e:
                lista_erros.append(names_files)
                continue
        
        if lista:
            df_temp = pd.concat(lista, axis= 0, ignore_index= True)
            df = df_temp.loc[df_temp['Retorno'] != 'SEM RETORNO'].copy()
            df = df.drop(columns= ['Unnamed: 11', 'Unnamed: 12','Unnamed: 13', 'Unnamed: 14'])
            df = df.dropna(subset=['END'])
            total_lista = len(lista)
            print("\nProcessamento de DataFrames concluído com sucesso.")
        else:
            print("\nNenhuma planilha válida foi encontrada para processamento.")
    except Exception as e:
        error = validar_erro(e)
        print(f"Tratamento: {error}\n")

    try:
        df.to_excel(output.cheio_vazio, index=False, sheet_name= "FATO_GERAL")

        total_erros = len(lista_erros)
        total_lista = len(lista)
        
        print(f"{total_lista} arquivos processados.\n")
        print(f"{total_erros} arquivos com divergencias.\n")
    except Exception as e:
        error = validar_erro(e)
        print(f"Carga: {error}\n")

if __name__ == "__main__":
    app = app()
    input("\nPressione Enter para sair...")
