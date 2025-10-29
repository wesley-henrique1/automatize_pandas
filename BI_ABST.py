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
        
class main:
    try:
        print("\nEXTRAÇÃO...\n")
        cons28 = pd.read_excel(bi_abst.cons_28)
        cons64 = pd.read_excel(bi_abst.cons_64)
        func = pd.read_excel(bi_abst.func, sheet_name='FUNC')
        m_atual28 = pd.read_excel(bi_abst.m_atual28)
        m_atual64 = pd.read_excel(bi_abst.m_atual64)
    except Exception as e:
        erros = validar_erro(e)
        print(f"EXTRAÇÃO: {erros}")

    try:
        print("\nTRATAMENTO...")

        try:
            func = func.loc[func['AREA'] == 'EXPEDICAO']
        except Exception as e:
            erros = validar_erro(e)
            print("\nTRATAMENTO_func: \n")

        try:
            m_atual28 = organizar_df(m_atual28,'DATA',1)
            cons28 = organizar_df(cons28,'DATA',1)
            cons28 = cons28.loc[cons28['MES'] != 10]
            cons28 = pd.concat([cons28, m_atual28], ignore_index= True)

            m_atual64 = organizar_df(m_atual64, 'DATAGERACAO', 2)
            cons64 = organizar_df(cons64, 'DATAGERACAO', 2)
            cons64 = cons64.loc[cons64['MES'] != 10]
            cons64 = pd.concat([cons64, m_atual64], ignore_index= True)

            grop28 = cons28.groupby(['DATA','CODFUNCOS','FUNCOSFIM']).agg(
                QTDE_OS = ("NUMOS", 'nunique'),
                OS_50 = ("Tipo O.S.", lambda x: (x == '50 - Movimentação De Para').sum()),
                OS_61 = ("Tipo O.S.", lambda x: (x == '61 - Movimentação De Para Horizontal').sum()),
                OS_58 = ("Tipo O.S.", lambda x: (x == '58 - Transferencia de Para Vertical').sum())
            ).reset_index()
            grop28['50_61'] = grop28['OS_50'] + grop28['OS_61']
            grop64 = cons64.groupby(['DATAGERACAO','CODEMPILHADOR']).agg(
                OS_RECEB = ("NUMOS", 'nunique')
            ).reset_index()
        except Exception as e:
            erros = validar_erro(e)
            print("\nTRATAMENTO_28-64: \n")

        try:
            os_temp28 = cons28[['NUMOS', 'DATA']].copy()
            os_temp64 = cons64[['NUMOS','DATAGERACAO']].copy()
            os_temp64 = os_temp64.rename(columns={'DATAGERACAO' : 'DATA'})
            os_geral = pd.concat([os_temp28,os_temp64], ignore_index= True)
            os_geral = os_geral.groupby('DATA').agg(
                TOTAL_OS = ('NUMOS', 'nunique')
            ).reset_index()
        except Exception as e:
            erros = validar_erro(e)
            print("\nTRATAMENTO_os_geral: \n")

        try:
            os_fim = func.merge(grop28, left_on='ID_FUNC', right_on='CODFUNCOS', how= 'left')
            os_fim = os_fim.merge(grop64, left_on=['ID_FUNC', 'DATA'], right_on= ['CODEMPILHADOR','DATAGERACAO'], how='left').drop(columns=['DATAGERACAO','CODEMPILHADOR', 'CODFUNCOS', 'FUNCOSFIM'])
            os_fim = os_fim.fillna(0)
            os_fim = os_fim.loc[os_fim['DATA'] != 0]
        except Exception as e:
            erros = validar_erro(e)
            print("\nTRATAMENTO_os_fim: \n")

        try:
            os_temp28 = cons28[['NUMOS', 'DATA']].copy()
            os_temp64 = cons64[['NUMOS','DATAGERACAO']].copy()
            os_temp64 = os_temp64.rename(columns={'DATAGERACAO' : 'DATA'})
            os_geral = pd.concat([os_temp28,os_temp64], ignore_index= True)
            os_geral = os_geral.groupby('DATA').agg(
                TOTAL_OS = ('NUMOS', 'nunique')
            ).reset_index()

            ped_28 = cons28.loc[cons28['POSICAO'] == "P"]
            ped_28 = ped_28.groupby(['CODFUNCGER','DATA']).agg(
                TOTAL_PD = ('NUMOS', 'nunique')
            ).reset_index()

            cons64['CODEMPILHADOR'] = cons64['CODEMPILHADOR'].fillna(0)
            cons64['CODFUNCGER'] =1
            ped_64 = cons64.loc[cons64['CODEMPILHADOR'] == 0]
            ped_64 = ped_64.rename(columns= {'DTLANC':'DATA'})
            ped_64 = ped_64.groupby(['CODFUNCGER','DATA']).agg(
                TOTAL_PD = ('NUMOS', 'nunique')
            ).reset_index()

            pd_geral = pd.concat([ped_28,ped_64], ignore_index= True)        
        except Exception as e:
            erros = validar_erro(e)
            print("\nTRATAMENTO_os_pedencia: \n")

        try:
            bonus = cons64.groupby(['DTLANC']).agg(
                TOTAL_BONUS = ("NUMBONUS", "nunique")
            ).reset_index().sort_values(by='DTLANC', ascending= False)
        except Exception as e:
            erros = validar_erro(e)
            print("\nTRATAMENTO_bonus: \n")

    except Exception as e:
        erros = validar_erro(e)
        print(f"TRATAMENTO: {erros}")

    try:
        print("\nCARGA...")
        cons28.to_excel(bi_abst.cons_28, index= False, sheet_name= "8628")
        cons64.to_excel(bi_abst.cons_64, index= False, sheet_name= "8664")
        os_geral.to_excel(bi_abst.os_geral, index= False, sheet_name= "OS_GERAL")
        os_fim.to_excel(bi_abst.os_fim, index= False, sheet_name= "OS_FIM")
        pd_geral.to_excel(bi_abst.os_pd, index= False, sheet_name= "OS_PD")
        bonus.to_excel(bi_abst.dim_bonus, index= False, sheet_name= "BONUS")       
    except Exception as e:
        erros = validar_erro(e)
        print(f"CARGA {erros}")


if __name__ == "__main__":
    main()
    input("\nPressione Enter para finalizar...")