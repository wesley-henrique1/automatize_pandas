from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np
import os

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

        end_ger = pd.read_csv(ar_csv.ar_07, header= None, names= col_name.c07)  
        df_bloq = pd.read_excel(ar_xls.ar_86, usecols=['Código', 'Bloqueado(Qt.Bloq.-Qt.Avaria)'])
        df_end = pd.read_csv(ar_csv.ar_end, header= None, names= col_name.cEnd)
        df_prod = pd.read_excel(ar_xlsx.ar_96, usecols= ['CODPROD', 'QTUNITCX','QTTOTPAL'])
    except Exception as e:
        erro = validar_erro(e)
        print(f"Etapa extração: {erro}")
        
    try:
        def ajuste_col(df_original, col):
            df_copia = df_original.copy() 
            for i in col:
                df_copia[i] = df_copia[i].fillna(0).astype(str)
                df_copia[i] = df_copia[i].str.replace(".", "")
                df_copia[i] = df_copia[i].str.replace(",", ".")
                df_copia[i] = df_copia[i].fillna(0).astype(float)
            return df_copia
        
        dic_end_ger = {'COD' : "CODPROD"}
        end_ger = end_ger.rename(columns= dic_end_ger)
        end_ger['RUA'] = end_ger['RUA'].fillna(0).astype(int)

        dic_bloq = {'Código' : "CODPROD", 'Bloqueado(Qt.Bloq.-Qt.Avaria)' : "BLOQ"}
        df_bloq = df_bloq.rename(columns= dic_bloq)
        df_bloq['CODPROD'] = df_bloq['CODPROD'].fillna(0).astype(int)

        dic_end = {"COD" : "CODPROD"}
        df_end = df_end.rename(columns= dic_end)
        df_end = df_end[['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']]

        
        col_ajuste = ['CODPROD','ENTRADA', 'SAIDA', 'DISP','QTDE']
        df_end = ajuste_col(df_end, col_ajuste)
        
        # for col in col_ajuste:
        #     df_end[col] = df_end[col].fillna(0).astype(str)
        #     df_end[col] = df_end[col].str.replace(".", "")
        #     df_end[col] = df_end[col].str.replace(",", ".")
        #     df_end[col] = df_end[col].fillna(0).astype(float)
        grupo_end = df_end.groupby('CODPROD').agg(
            SAIDA = ('SAIDA', 'sum'),
            ENTRADA = ('ENTRADA', 'sum'),
            DISP = ('DISP', 'sum'),
            QTDE = ('QTDE', 'sum'),
        ).reset_index()

        df_prod['CODPROD'] = df_prod['CODPROD'].fillna(0).astype(int)

        df = end_ger.loc[end_ger['RUA'].between(1, 39)]
        df = df.merge(df_bloq, left_on='CODPROD',right_on='CODPROD', how= 'left')
        df = df.merge(grupo_end, left_on='CODPROD',right_on='CODPROD', how= 'left')
        df = df.merge(df_prod, left_on='CODPROD',right_on='CODPROD', how= 'left')
        drop_col = ['EMBALAGEM']
        df.drop(columns= drop_col, inplace= True)

        col_ajuste = ['ENDERECO', 'GERENCIAL','PICKING','CAP','QTUNITCX','ENTRADA', 'SAIDA', 'QTDE']
        for col in col_ajuste:
            df[col] = df[col].fillna(0).astype(str)
            df[col] = df[col].str.replace(".", "")
            df[col] = df[col].str.replace(",", ".")
            df[col] = df[col].fillna(0).astype(float)

        df['DIF_UN'] = df['ENDERECO'].astype(float) - df['GERENCIAL'].astype(float)
        df['DIF_CX'] = (df['DIF_UN'] / df['QTUNITCX'].astype(int)).round(1)
        df['ENDERECO'] = df['ENDERECO'] + df['ENTRADA']
        df['CAP_CONVERTIDA'] = df['CAP'] * df['QTUNITCX']
        df['PENDENCIA'] = np.where(df['QTDE_O.S'] > 0, 'FOLHA', 'INVENTARIO')

        cond_dif = [
            df['DIF_CX'] == 0,
            # POSITIVO
            (df['DIF_CX'] > 0) & (df['DIF_CX'] < 5),
            (df['DIF_CX'] >= 5) & (df['DIF_CX'] < 10),
            df['DIF_CX'] >= 10,
            # NEGATIVO
            (df['DIF_CX'] < 0) & (df['DIF_CX'] > -5),
            (df['DIF_CX'] <= -5) & (df['DIF_CX'] > -10),
            df['DIF_CX'] <= -10
        ]
        result_dif = ['0', '1', '2', '3', '-1', '-2', '-3',]

        cond_op = [
            df['DIF_UN'] < 0,
            df['DIF_UN'] > 0,
            df['DIF_UN'] == 0,
        ]
        result_op = ["END_MENOR", "END_MAIOR", "CORRETO"]

        cond_ap = [
            df['PICKING'].astype(int) > df['CAP_CONVERTIDA'].astype(int),
            df['PICKING'].astype(int) < 0,
            df['PICKING'].astype(int) == 0
        ]
        result_ap = [
            'AP_MAIOR',
            "AP_NEGATIVO",
            "CORRETO"
        ]

        df['CATEGORIA_DIF'] = np.select(cond_dif, result_dif, default= "-").astype(int)
        df["TIPO_OP"] = np.select(cond_op, result_op, default= "-")
        df['AP_VS_CAP'] = np.select(cond_ap, result_ap, default= "-")
    except Exception as e:
        erro = validar_erro(e)
        print(f"Etapa tratamento: {erro}")

    try:
        df[['RUA', 'PREDIO']] = df[['RUA', 'PREDIO']].astype(int)
        df = df.sort_values(by=['RUA', 'PREDIO'], ascending= True)

        path_div = os.path.join(files_bi.bi_str, "FATO_DIVERGENCIA.xlsx")
        df.to_excel(path_div, index= False, sheet_name= 'DIVERGENCIA')
    except Exception as e:
        erro = validar_erro(e)
        print(f"Etapa carga: {erro}")



if __name__ == '__main__':
    app()
    input("Aperte 'enter' para finalizar o processo...")