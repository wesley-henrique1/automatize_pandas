from config.config_path import *
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

def agrupar(df, col, id):
    if id == 1:
        temp = df.groupby(col).agg(
            QTDE_OS = ("NUMOS", 'nunique'),
            OS_50 = ("Tipo O.S.", lambda x: (x == '50 - Movimentação De Para').sum()),
            OS_61 = ("Tipo O.S.", lambda x: (x == '61 - Movimentação De Para Horizontal').sum()),
            OS_58 = ("Tipo O.S.", lambda x: (x == '58 - Transferencia de Para Vertical').sum())
        ).reset_index()
        return temp
    elif id == 2:
        temp = df.groupby(col).agg(
             OS_RECEB = ("NUMOS", 'nunique')
        ).reset_index()
        return temp
    elif id == 3:
        temp = df.groupby(col).agg(
            OS_RECEB = ('OS_RECEB', 'sum')
        ).reset_index()
        temp[col] = pd.to_datetime(temp[col]).dt.normalize()
        return temp
    elif id == 4:
        temp = df.groupby(col).agg(
            QTDE_OS = ("QTDE_OS", 'sum'),
            OS_50 = ("OS_50", 'sum'),
            OS_61 = ("OS_61", 'sum'),
            OS_58 = ("OS_58", 'sum')
        ).reset_index()
        temp[col] = pd.to_datetime(temp[col]).dt.normalize()
        return temp 
    elif id == 5:
        temp = df.groupby([col]).agg(
                TOTAL_BONUS = ("NUMBONUS", "nunique")
            ).reset_index().sort_values(by=col, ascending= False)  
        return temp 

class main:
    try:
        print("\nEXTRAÇÃO...\n")
        func = pd.read_excel(outros.ou_func, sheet_name='FUNC')
        cons28 = pd.read_excel(power_bi.abst_cons28)
        m_atual28 = pd.read_excel(power_bi.abst_atual28)
        cons64 = pd.read_excel(power_bi.abst_cons64)
        m_atual64 = pd.read_excel(power_bi.abst_atual64)
        configurar_mes = 11
    except Exception as e:
        erros = validar_erro(e)
        print(f"EXTRAÇÃO: {erros}")

    try:
        print("\nTRATAMENTO...")

        try:
            m_atual64['CODEMPILHADOR'] = m_atual64['CODEMPILHADOR'].fillna(0)   
            m_atual28 = organizar_df(m_atual28,'DATA',1)
            cons28 = organizar_df(cons28,'DATA',1)
            cons28 = cons28.loc[cons28['MES'] != configurar_mes]
            concat28 = pd.concat([cons28, m_atual28], ignore_index= True)

            os_geradas28 = agrupar(concat28, ['DATA','CODFUNCGER'], 1)
            os_finalizadas28 = agrupar(concat28, ['DATA','CODFUNCOS'], 1)

            pendencia = concat28.loc[concat28['POSICAO'] =='P']
            os_pedentes28 = agrupar(pendencia, ['DATA','CODFUNCGER'], 1)        
        except Exception as e:
            erros = validar_erro(e)
            print("\nTRATAMENTO_28: \n")


        try:
            m_atual64 = organizar_df(m_atual64, 'DATAGERACAO', 2)
            cons64 = organizar_df(cons64, 'DATAGERACAO', 2)

            cons64 = cons64.loc[cons64['MES'] != configurar_mes]
            concat64 = pd.concat([cons64, m_atual64], ignore_index= True)
            concat64 = concat64.rename(columns={'DATAGERACAO' : "DATA"})

            agrupamento = agrupar(concat64, ['DATA', 'CODEMPILHADOR'], 2).fillna(0)
            agrupamento['CODFUNCGER'] = 1

            os_finalizadas64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] != 0]
            os_pedentes64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] == 0]
        except Exception as e:
            erros = validar_erro(e)
            print(f"\nTRATAMENTO_64: {e}\n")

        try:
            print("CONCATENAÇÃO")
            func = func.loc[func['AREA'] == 'EXPEDICAO']

            """Ordem de serviço pendentes"""
            temp28 = os_pedentes28[['DATA','CODFUNCGER','QTDE_OS']]
            temp64 = os_pedentes64[['DATA','CODFUNCGER','OS_RECEB']]
            temp64 = temp64.rename(columns={ 'OS_RECEB': 'QTDE_OS'})
            pd_total = pd.concat([temp28, temp64], ignore_index= True)

            """Ordem de serviço finalizadas"""
            os_finalizadas64 = os_finalizadas64.rename(columns={'CODEMPILHADOR': 'CODFUNCOS'})
            fim_total = os_finalizadas28.merge(os_finalizadas64, left_on=['DATA','CODFUNCOS'], right_on= ['DATA','CODFUNCOS'], how= 'left').drop(columns='CODFUNCGER').fillna(0)

            """Ordem de serviço geradas"""
            grupo64 = agrupar(agrupamento, 'DATA', 3)
            validar2 = grupo64.loc[grupo64['DATA'] == '2025-10-27']
            print(f"\n{"validar":-^57}")
            print(validar2)
            print("-" * 57)

            grupo28 = agrupar(os_geradas28, 'DATA', 4)
            geral_total = grupo28.merge(grupo64, left_on='DATA', right_on= 'DATA', how= 'inner').fillna(0)

            bonus = agrupar(concat64, 'DTLANC', 5)
        except Exception as e:
            erros = validar_erro(e)
            print(f"\nTRATAMENTO_CONCATENAÇÃO: {erros}\n")
    except Exception as e:
        erros = validar_erro(e)
        print(f"\nTRATAMENTO: {erros}\n")

    try:
        print("\nCARGA...")
        
        pd_total.to_excel(power_bi.abst_pd, index= False, sheet_name= "OS_PD")
        fim_total.to_excel(power_bi.abst_fim, index= False, sheet_name= "OS_FIM")
        geral_total.to_excel(power_bi.abst_geral, index= False, sheet_name= "OS_GERAL")
        bonus.to_excel(power_bi.abst_bonus, index= False, sheet_name= "BONUS")       
    except Exception as e:
        erros = validar_erro(e)
        print(f"CARGA {erros}")


if __name__ == "__main__":
    main()
    input("\nPressione Enter para finalizar...")