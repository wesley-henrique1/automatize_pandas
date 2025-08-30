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
        print("=========INICIANDO PROCESSO DE LEITURA=========")
        deposito = glob.glob(os.path.join(pasta.p_82, 'DEP*.txt'))
        df_DEP = pd.DataFrame([])
        movi = glob.glob(os.path.join(pasta.p_82, 'MOV*.txt'))
        df_mov = pd.DataFrame([])
        print("\nprocesso finalizado\n")
        print("listas de arquivos:")
        print(f"depositos: {deposito}")
        print(f"movimentação: {movi}")
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

    try:
        def loop_ar(ar,df):
            lista = []
            for arquivo in ar:
                x = pd.read_csv(arquivo, header= None)
                lista.append(x)
            df_final = pd.concat(lista, axis= 0, ignore_index= True)
            return df_final

        df_DEP = loop_ar(deposito, df_DEP)
        df_mov = loop_ar(movi, df_mov)
        df_DEP.columns = col_name.c82
        df_mov.columns = col_name.cMovi

        with pd.ExcelWriter(output.dep_82) as writer:
            df_DEP.to_excel(writer, engine='openpyxl', index= False, sheet_name= "depositos")
            df_mov.to_excel(writer, engine= 'openpyxl', index= False, sheet_name= "MOVI")
    except Exception as e:
        error = validar_erro(e)
        print(error)
        exit()

if __name__ == "__main__":
    app()
    print('fim do processo, verifique o arquivo')
    input("\nPressione Enter para sair...")

