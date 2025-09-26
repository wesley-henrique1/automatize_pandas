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
        df_life = pd.read_excel(totvs.life, usecols=['CONFERENCIA','PRODUTO','VALIDADE','PROBLEMA'])
        df_bonus = pd.read_excel(ar_xlsx.ar_64)
        df_end = pd.read_csv(ar_csv.ar_end, header= None, names= col_name.cEnd)
        df_str = pd.read_excel(ar_xlsx.ar_str, sheet_name= "AE", usecols= ['COD_END', 'RUA'])
    except Exception as e:
        error = validar_erro(e)
        print(F"EXTRAÇÃO: {error}")

    try:
        df_life['COD'] = (df_life['PRODUTO'].str.split('-').str[0]).astype(int)
        df_life['VALIDADE'] = pd.to_datetime(df_life['VALIDADE'], errors='coerce')
        df_life["ID_LIFE"] = df_life['CONFERENCIA'].astype(str) + " - " + df_life['COD'].astype(str) + " - " + df_life['VALIDADE'].astype(str)

        df_prod = df_bonus[['NUMBONUS', 'CODPROD', 'DESCRICAO', 'CODENDERECO', 'DTVALIDADE']].copy()
        df_prod['DTVALIDADE'] = pd.to_datetime(df_prod['DTVALIDADE'], errors='coerce')
        df_prod["ID_LIFE"] = df_prod['NUMBONUS'].astype(str) + " - " + df_prod['CODPROD'].astype(str) + " - " + df_prod['DTVALIDADE'].astype(str)

        merge = df_life.merge(df_prod, left_on='ID_LIFE', right_on='ID_LIFE', how= 'inner')
        merge = merge[['CODPROD', 'DESCRICAO', 'CODENDERECO','NUMBONUS', 'DTVALIDADE','PROBLEMA']]
        merge['ID_END'] = merge['CODENDERECO'].astype(str) + " - " + merge['CODPROD'].astype(str)

        aereo = df_end.merge(df_str, left_on='COD_END', right_on='COD_END', how='left')
        aereo['ID_END'] = aereo['COD_END'].astype(str) + " - " + aereo['COD'].astype(str)
        aereo = aereo[['COD','COD_END','RUA','PREDIO', 'NIVEL', 'APTO', 'DT.VALIDADE','ID_END']]

        df_fim = merge.merge(aereo, left_on='ID_END', right_on='ID_END', how= 'inner')
        df_fim = df_fim[['CODPROD', 'DESCRICAO', 'NUMBONUS', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'DTVALIDADE','DT.VALIDADE','PROBLEMA']]
    except Exception as e:
        error = validar_erro(e)
        print(F"TRATAMENTO: {error}")

    try:
        df_fim.to_excel(output.totvs_life, sheet_name="DIVERGENCIA", index=False)
    except Exception as e:
        error = validar_erro(e)
        print(F"CARGA: {error}")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")