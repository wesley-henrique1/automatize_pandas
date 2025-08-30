from OUTROS.path_arquivos import *
import pandas as pd

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
        df = pd.read_excel(ar_xlsx.ar_96)
        df_filtro = df.loc[df['RUA'].between(1, 39)].copy()
        df_filtro['DTULTENT'] = pd.to_datetime(df_filtro['DTULTENT'], errors='coerce')
        df_filtro['DTULTENT'] = df_filtro['DTULTENT'].fillna(value='1900-01-01')
        df_filtro.to_excel(output.filtro_96, index= False, sheet_name= 'filtrado')
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

if __name__ == '__main__':
    app()
    input("\nPressione Enter para sair...")