from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np

def validar_erro(e):
    print("=" * 60)
    if isinstance(e, KeyError):
        return f"KeyError: A coluna ou chave '{e}' não foi encontrada."
    elif isinstance(e, PermissionError):
        return f"PermissionError: O arquivo está sendo usado ou você não tem permissão para acessá-lo. Por favor, feche o arquivo."
    elif isinstance(e, ValueError):
        return f"ValueError: Erro de valor. Mensagem original: {e}"
    elif isinstance(e, TypeError):
        return f"TypeError: Erro de tipo. Verifique se os dados são do tipo correto. Mensagem original: {e}"
    else:
        return f"Ocorreu um erro inesperado: {e}"
    
def app():
    try:
        print("=" * 60)
        print("Processo iniciado, favor aguarde...\n")
        df_bloq = pd.read_excel(ar_xls.ar_86, usecols=['Código', 'Bloqueado(Qt.Bloq.-Qt.Avaria)'])
        df_prod = pd.read_excel(ar_xlsx.ar_96, usecols= ['CODPROD', 'QTUNITCX', 'CAPACIDADE', 'PONTOREPOSICAO','QTTOTPAL'])

        df_end = pd.read_csv(ar_csv.ar_end, header= None, names= col_name.cEnd)
        grupo = df_end.groupby('COD').agg(
        ENTRADA = ('ENTRADA', 'sum'),
        SAIDA = ('SAIDA', 'sum'),
        QTDE = ('COD_END', 'count')
        ).reset_index()

        df_07 = pd.read_csv(ar_csv.ar_07, header=None, names= col_name.c07)
        df_07 = df_07[df_07['RUA'].between(1, 39)]

        df = df_07.merge(df_prod, left_on= 'COD', right_on= 'CODPROD', how= 'inner').drop(columns= 'CODPROD')
        df = df.merge(grupo, left_on= 'COD', right_on= 'COD', how= 'left')
        df= df.merge(df_bloq, left_on= 'COD', right_on= 'Código').drop(columns= 'Código')
    except Exception as e:
        erro = validar_erro(e)
        print(f"Etapa extração: {erro}")
        exit()

    try:
        col = ['ENDERECO', 'GERENCIAL','PICKING','CAPACIDADE','QTUNITCX','ENTRADA', 'SAIDA', 'QTDE']
        for coluna in col:
            df[coluna] = df[coluna].astype(str)
            df[coluna] = df[coluna].str.replace('.' , '')
            df[coluna] = df[coluna].str.replace(',', '.')
            df[coluna] = df[coluna].fillna(0).astype(float)

        df['ENDERECO'] = df['ENDERECO'] + df['ENTRADA']
        df['DIVERGENCIA'] = df['ENDERECO'].astype(float) - df['GERENCIAL'].astype(float)
        df["TIPO_OP"] = np.where(df['DIVERGENCIA'] < 0,"END_MENOR"
                        ,np.where(df['DIVERGENCIA'] > 0,"END_MAIOR"
                        ,np.where(df['DIVERGENCIA'] == 0, "CORRETO","-")))
        df['CAP_CONVERTIDA'] = df['CAPACIDADE'] * df['QTUNITCX']


        df['AP_VS_CAP'] = np.where(df['PICKING'].astype(int) > df['CAP_CONVERTIDA'].astype(int),'AP_MAIOR'
                        ,np.where(df['PICKING'].astype(int) < 0, "AP_NEGATIVO","CORRETO"))
        df['PENDENCIA'] = np.where(df['QTDE_O.S'] > 0, 'FOLHA', 'INVENTARIO')
        df['RUA'] = df['RUA'].astype(int)


        df.to_excel(output.divergencia, index= False, sheet_name= 'DIVERGENCIA')
    except Exception as e:
        erro = validar_erro(e)
        print(f"Etapa 2: {erro}")
        exit()

    try:
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
    except Exception as e:
        erro = validar_erro(e)
        print(f"Etapa 3: {erro}")
        exit()

if __name__ == '__main__':
    app()
    input("Aperte 'enter' para finalizar o processo...")