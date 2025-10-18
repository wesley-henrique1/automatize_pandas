from OUTROS.path_arquivos import pasta, bi_abst
import pandas as pd
import numpy as pn
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
        pass
    except Exception as e:
        erros = validar_erro(e)
        print(f"EXTRAÇÃO: {erros}")

    try:
        pass
    except Exception as e:
        erros = validar_erro(e)
        print(f"TRATAMENTO: {erros}")

    try:
        pass
    except Exception as e:
        erros = validar_erro(e)
        print(f"CARGA")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para finalizar...")