from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np

def app():
    df_f18 = pd.read_excel(ar_xls.fl_18, usecols= ['Código', 'Estoque'])
    df = pd.read_csv(ar_csv.ar_07, header=None, names= col_name.c07)
    df_prod = pd.read_excel(ar_xlsx.ar_96, usecols= ['CODPROD', 'QTUNITCX', 'CAPACIDADE', 'PONTOREPOSICAO'])
    df = df[df['RUA'].between(1, 39)]
    df = df.merge(df_prod, left_on= 'COD', right_on= 'CODPROD', how= 'inner')
    df = df.merge(df_f18, left_on= 'COD', right_on='Código', how= 'left')

    col = ['ENDERECO', 'GERENCIAL']
    for coluna in col:
        df[coluna] = df[coluna].str.replace('.' , '')
        df[coluna] = df[coluna].str.replace(',', '.').astype(float)
    df['DIVERGENCIA'] = df['ENDERECO'].astype(int) - df['GERENCIAL'].astype(int)

    df["TIPO_OP"] = np.where(df['DIVERGENCIA'] < 0,"END_MENOR"
                    ,np.where(df['DIVERGENCIA'] > 0,"END_MAIOR"
                    ,np.where(df['DIVERGENCIA'] == 0, "CORRETO","-")))
    df['CAP_CONVERTIDA'] = df['CAPACIDADE'] * df['QTUNITCX']
    df['AP_VS_CAP'] = np.where(df['PICKING'].astype(int) > df['CAP_CONVERTIDA'].astype(int) ,'AP_MAIOR'
                    ,np.where(df['PICKING'].astype(int) < 0, "AP_NEGATIVO","CORRETO"))

    df.to_excel(output.divergencia, index= False)

    x = df.groupby('TIPO_OP').agg(
        qtde_itens = ('DESCRICAO', 'count')
    )
    y = df.groupby('AP_VS_CAP').agg(
        qtde_itens = ('DESCRICAO', 'count')
    )
    print("=" * 60)
    print("COMPARATIVO ENDEREÇADO VS GERENCIAL")
    print("=" * 60)
    print(x)
    print("\n")
    print(y)
    print("=" * 60)

if __name__ == '__main__':
    app()
    input("Aperte 'enter' para finalizar o processo...")