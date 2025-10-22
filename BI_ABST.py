from OUTROS.path_arquivos import pasta, bi_abst, ar_xlsx
import pandas as pd
import numpy as pn
import glob
import os
a = 0
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
        print("EXTRAÇÃO")
        cons28 = pd.read_excel(bi_abst.cons_28)
        cons64 = pd.read_excel(bi_abst.cons_64)
        funcionario = pd.read_excel(ar_xlsx.ar_func, sheet_name='FUNC')
    except Exception as e:
        erros = validar_erro(e)
        print(f"EXTRAÇÃO: {erros}")

    try:
        print("TRATAMENTO")
        cons28['DATA'] = pd.to_datetime(cons28['DATA']).dt.normalize()
        cons28['MES'] = cons28['DATA'].dt.month
        cons28 = cons28.loc[(cons28['MES'] != 10) & ((cons28['CODROTINA'] == 1709) | (cons28['CODROTINA'] == 1723))]

        grop28 = cons28.groupby(['DATA','CODFUNCOS','FUNCOSFIM']).agg(
            QTDE_OS = ("NUMOS", 'nunique'),
            OS_50 = ("Tipo O.S.", lambda x: (x == '50 - Movimentação De Para').sum()),
            OS_61 = ("Tipo O.S.", lambda x: (x == '61 - Movimentação De Para Horizontal').sum()),
            OS_58 = ("Tipo O.S.", lambda x: (x == '58 - Transferencia de Para Vertical').sum())
        ).reset_index()

        cons64['DATAGERACAO'] = pd.to_datetime(cons64['DATAGERACAO']).dt.normalize()
        cons64 = cons64.drop_duplicates(subset=['NUMOS'])
        cons64['MES'] = cons64['DATAGERACAO'].dt.month
        cons64 = cons64.loc[cons64['MES'] != 10]

        grop64 = cons64.groupby(['DATAGERACAO','CODEMPILHADOR']).agg(
            OS_RECEB = ("NUMOS", 'nunique')
        ).reset_index()











        df = funcionario['ID_NOME', 'NOME', 'AREA', 'NAME', 'SETOR'].copy()

        
        
    except Exception as e:
        erros = validar_erro(e)
        print(f"TRATAMENTO: {erros}")

    try:
        print("CARGA")

        # cons28.to_excel(bi_abst.cons_28, index= False, sheet_name= "8628")
        cons64.to_excel(bi_abst.cons_64, index= False, sheet_name= "8664")
    except Exception as e:
        erros = validar_erro(e)
        print(f"CARGA {erros}")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para finalizar...")