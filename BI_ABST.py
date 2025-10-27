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

def organizar_df(df_original, col, id):
    df = df_original
    df[col] = pd.to_datetime(df[col]).dt.normalize()
    df['MES'] = df[col].dt.month

    if id == 1:
        df = df[['NUMOS', 'DATA', 'CODROTINA', 'POSICAO', 'CODFUNCGER', 'FUNCGER','DTFIMOS', 'CODFUNCOS', 'FUNCOSFIM', 'Tipo O.S.', 'TIPOABAST', 'MES']].copy()
        df = df.loc[(df['CODROTINA'] == 1709) | (df['CODROTINA'] == 1723)]
        return df
    elif id ==2: 
        df = df[['DATAGERACAO', 'DTLANC', 'NUMBONUS', 'NUMOS', 'CODEMPILHADOR','EMPILHADOR', 'MES']].copy()
        df = df.drop_duplicates(subset=['NUMOS'])
        return df
        
def app():
    try:
        print("\nEXTRAÇÃO...\n")
        cons28 = pd.read_excel(bi_abst.cons_28)
        cons64 = pd.read_excel(bi_abst.cons_64)
        funcionario = pd.read_excel(ar_xlsx.ar_func, sheet_name='FUNC')
        m_atual28 = pd.read_excel(bi_abst.m_atual28)
        m_atual64 = pd.read_excel(bi_abst.m_atual64)
    except Exception as e:
        erros = validar_erro(e)
        print(f"EXTRAÇÃO: {erros}")

    try:
        print("\nTRATAMENTO...\n")
        func = funcionario[['ID_NOME', 'NOME', 'AREA', 'NAME', 'SETOR']].copy()
        func = func.loc[func['AREA'] == 'EXPEDICAO']

        m_atual28 = organizar_df(m_atual28,'DATA',1)
        m_atual64 = organizar_df(m_atual64, 'DATAGERACAO', 2)

        cons28 = organizar_df(cons28,'DATA',1)
        cons28 = cons28.loc[cons28['MES'] != 10]
        cons64 = organizar_df(cons64, 'DATAGERACAO', 2)
        cons64 = cons64.loc[cons64['MES'] != 10]


        grop28 = cons28.groupby(['DATA','CODFUNCOS','FUNCOSFIM']).agg(
            QTDE_OS = ("NUMOS", 'nunique'),
            OS_50 = ("Tipo O.S.", lambda x: (x == '50 - Movimentação De Para').sum()),
            OS_61 = ("Tipo O.S.", lambda x: (x == '61 - Movimentação De Para Horizontal').sum()),
            OS_58 = ("Tipo O.S.", lambda x: (x == '58 - Transferencia de Para Vertical').sum())
        ).reset_index()

        grop64 = cons64.groupby(['DATAGERACAO','CODEMPILHADOR']).agg(
            OS_RECEB = ("NUMOS", 'nunique')
        ).reset_index()

        df_os_geral = func.merge(grop28, left_on='ID_NOME', right_on='CODFUNCOS', how= 'left')
        df_os_geral = df_os_geral.merge(grop64, left_on=['ID_NOME', 'DATA'], right_on= ['CODEMPILHADOR','DATAGERACAO'], how='left').drop(columns=['DATAGERACAO','CODEMPILHADOR', 'CODFUNCOS', 'FUNCOSFIM'])

        df_os_geral = df_os_geral.fillna(0)
        df_os_geral = df_os_geral.loc[df_os_geral['DATA'] != 0]
        df_os_geral['DATA'] = pd.to_datetime(df_os_geral['DATA']).dt.normalize()
        df_os_geral['MES'] = df_os_geral['DATA'].dt.month
        df_os_geral = df_os_geral.loc[df_os_geral['MES'] == 8]

        bonus = cons64.groupby(['NUMBONUS','DTLANC']).agg(
            TOTAL_OS = ("NUMOS", "nunique")
        ).reset_index().sort_values(by='DTLANC', ascending= False)
        

    except Exception as e:
        erros = validar_erro(e)
        print(f"TRATAMENTO: {erros}")

    try:
        print("CARGA")
        bonus.to_excel(bi_abst.dim_bonus, index= False, sheet_name= "DIM_BONUS")
        df_os_geral.to_excel(bi_abst.acum_os, index= False, sheet_name= "FATO_OS")
        cons28.to_excel(bi_abst.cons_28, index= False, sheet_name= "8628")
        cons64.to_excel(bi_abst.cons_64, index= False, sheet_name= "8664")
    except Exception as e:
        erros = validar_erro(e)
        print(f"CARGA {erros}")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para finalizar...")