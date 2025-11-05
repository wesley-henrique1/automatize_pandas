from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np

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
        df_28 = pd.read_excel(ar_xlsx.ar_28, usecols= ['NUMOS','CODPROD', 'DESCRICAO', 'ENDERECO_ORIG', 'RUA', 'PREDIO', 'NIVEL', 'APTO','RUA_1', 'PREDIO_1', 'NIVEL_1', 'APTO_1', 'QT', 'POSICAO', 'CODFUNCOS', 'CODFUNCESTORNO', 'Tipo O.S.', 'FUNCOSFIM','FUNCGER'])
        df_28 = df_28.loc[(df_28['NIVEL'].between(2,8)) & (df_28['Tipo O.S.'] == '58 - Transferencia de Para Vertical')]
        df_28[['CODFUNCESTORNO', 'CODFUNCOS']] = df_28[['CODFUNCESTORNO', 'CODFUNCOS']].fillna(0).astype(int)
        df_28['ID_COD'] = df_28['ENDERECO_ORIG'].astype(str) + " - " + df_28['CODPROD'].astype(str)

        df_aereo = pd.read_csv(ar_csv.ar_end, header= None, names= col_name.cEnd)
        df_aereo = df_aereo.loc[df_aereo['TIPO_PK'] == 'AE']
        df_aereo = df_aereo[['COD_END', 'COD', 'QTDE', 'ENTRADA', 'SAIDA']]
        df_aereo['ID_COD'] = df_aereo['COD_END'].astype(str) + " - " + df_aereo['COD'].astype(str)

        df_func = pd.read_excel(ar_xlsx.ar_func, sheet_name= 'FUNC', usecols= ['ID_FUNC', 'SETOR'])
        
        df = df_28.merge(df_aereo, left_on= 'ID_COD', right_on= 'ID_COD', how= 'left').infer_objects(copy=False).fillna(0).drop(['COD_END', 'COD'],axis= 1)
        df = df.merge(df_func, left_on= 'CODFUNCOS', right_on= 'ID_FUNC', how= 'left').drop(columns=['ID_FUNC'])
        df['SETOR'] = df['SETOR'].fillna("FUNC")    
    except Exception as e:
        error = validar_erro(e)
        print(f"Etapa 1: {error}")

    try:

        df['QT'] = df['QT'].astype(str).str.replace(".", "")
        df['QT'] = df['QT'].astype(str).str.replace(",", ".")
        df['QTDE'] = df['QTDE'].astype(str).str.replace(".", "")
        df['QTDE'] = df['QTDE'].astype(str).str.replace(",", ".")

        df['TIPO_OP'] = np.where(df['QT'].astype(float) >= df['QTDE'].astype(float), "baixa", "retorno")

        df_pd = df.loc[(df['CODFUNCOS'] == 0) & (df['CODFUNCESTORNO'] == 0)]
        df_pd = df_pd[['NUMOS','CODPROD', 'DESCRICAO', 'RUA', 'PREDIO', 'NIVEL',
        'APTO', 'RUA_1', 'PREDIO_1', 'NIVEL_1', 'APTO_1','FUNCGER','TIPO_OP']].copy()
        df_pd = df_pd.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)

        df_fim = df.loc[(df['SETOR'] != 'RECEBIMENTO') & (df['SETOR'] != 'EMPILHADOR') & (df['SETOR'] != 'ABASTECEDOR') & (df['CODFUNCOS'] > 0) & (df['CODFUNCESTORNO']== 0) & (df['QTDE'].astype(float) == 0)]
        df_fim = df_fim[['NUMOS','CODPROD', 'DESCRICAO', 'RUA', 'PREDIO', 'NIVEL','APTO', 'RUA_1', 'PREDIO_1', 'NIVEL_1', 'APTO_1','FUNCOSFIM','TIPO_OP']].copy()
        df_fim = df_fim.sort_values(by=['RUA', 'PREDIO'], ascending= True, axis= 0)  
    except Exception as e:
        error = validar_erro(e)
        print(f"Etapa 2: {error}")

    try:
        with pd.ExcelWriter(output.val_baixa) as destino:
            df_pd.to_excel(destino, index= False, sheet_name= 'PEDENTES')
            df_fim.to_excel(destino, index= False, sheet_name= 'FIM_MESA')
    except Exception as e:
        error = validar_erro(e)
        print(f"Etapa 3: {error}")

if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")