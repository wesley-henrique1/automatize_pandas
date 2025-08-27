from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np

def app():
    try:
        df_prod = pd.read_excel(ar_xlsx.ar_96, usecols= ['CODPROD', 'QTUNITCX', 'CAPACIDADE', 'PONTOREPOSICAO','QTTOTPAL'])
        # df_f18 = pd.read_excel(ar_xls.fl_18, usecols= ['Código', 'Estoque'])
        df_07 = pd.read_csv(ar_csv.ar_07, header=None, names= col_name.c07)
        df_end = pd.read_csv(ar_csv.ar_end, header= None, names= col_name.cEnd)

        df = df_07[df_07['RUA'].between(1, 39)]
        df = df.merge(df_prod, left_on= 'COD', right_on= 'CODPROD', how= 'inner').drop(columns= 'CODPROD')
        # df = df.merge(df_f18, left_on= 'COD', right_on='Código', how= 'left').drop(columns= 'Código')
        col = ['ENDERECO', 'GERENCIAL']
        for coluna in col:
            df[coluna] = df[coluna].str.replace('.' , '')
            df[coluna] = df[coluna].str.replace(',', '.').astype(float)
    except Exception as e:
        print(f'Erro na tratativa dos DATAFRAME. {e}')


    df['DIVERGENCIA'] = df['ENDERECO'].astype(int) - df['GERENCIAL'].astype(int)
    df["TIPO_OP"] = np.where(df['DIVERGENCIA'] < 0,"END_MENOR"
                    ,np.where(df['DIVERGENCIA'] > 0,"END_MAIOR"
                    ,np.where(df['DIVERGENCIA'] == 0, "CORRETO","-")))
    df['CAP_CONVERTIDA'] = df['CAPACIDADE'] * df['QTUNITCX']
    df['AP_VS_CAP'] = np.where(df['PICKING'].astype(int) > df['CAP_CONVERTIDA'].astype(int) ,'AP_MAIOR'
                    ,np.where(df['PICKING'].astype(int) < 0, "AP_NEGATIVO","CORRETO"))
    df['PENDENCIA'] = np.where(df['QTDE_O.S'] > 0, 'FOLHA', 'INVENTARIO')
    df['RUA'] = df['RUA'].astype(int)


    df.to_excel(output.divergencia, index= False, sheet_name= 'DIVERGENCIA')


    """demostrativos"""
    end_menor = df.loc[df['TIPO_OP'] == 'END_MENOR']
    end_maior = df.loc[df['TIPO_OP'] == 'END_MAIOR']

    menor = end_menor.groupby('RUA').agg(
        qtde_itens = ('TIPO_OP', 'count')
    ).reset_index().sort_values('qtde_itens', ascending= False, axis= 0)
    maior = end_maior.groupby('RUA').agg(
        qtde_itens = ('TIPO_OP', 'count')
    ).reset_index().sort_values('qtde_itens', ascending= False, axis= 0)

    print("###### DEMONSTRATIVO ######\n")
    print('ENDEREÇADO MENOR QUE GERENCIAL')
    print(menor)
    print("\nENDEREÇADO MAIOR QUE GERENCIAL")
    print(maior)

if __name__ == '__main__':
    app()
    input("Aperte 'enter' para finalizar o processo...")