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
    try:    # ETAPA 1: LEITURA DOS DATAFRAMES
        print("\nIniciando controle de cadastro, favor aguarde...\n")
        base = pd.read_excel(ar_xlsx.ar_96)
        print("=" * 60)
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 1: {error}\n")
    
    try:    # ETAPA 2: AJUSTADO OS DATAFRAMES PARA FAZER AS ANALISE
        df = base.loc[base['RUA'].between(1,39)].copy()
        dic_raname = {
            'ABASTECEPALETE' : 'FLEG_ABST',
            'CAPACIDADE' : 'CAP'
        }
        df = df.rename(columns=dic_raname)
        concat = df['RUA'].astype(str) + " - " + df['PREDIO'].astype(str)
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 2: {error}\n")

    try:# ETAPA 3: CRIAÇÃO DAS COLUNAS CALCULADAS
        df['extrator'] = df['EMBALAGEMMASTER'].str.extract(r'(\d+)').astype(int)
        df['volume_master'] = df['ALTURAARM'].astype(float) * df['LARGURAARM'].astype(float) * df['COMPRIMENTOARM'].astype(float) 
        df['volume_venda'] = (df['ALTURAM3'].astype(float) * df['LARGURAM3'].astype(float) * df['COMPRIMENTOM3'].astype(float)) * df['extrator']
        df['CONT_AP'] = concat.map(concat.value_counts())
        
        list_int = ['2-INTEIRO(1,90)', '1-INTEIRO (2,55)']
        df['STATUS_PROD'] = np.where((df['CONT_AP'] <= 2) & (df['PK_END'].isin(list_int)), "INT", 
            np.where(df['CONT_AP'] > 3,"DIV", "VAL"))

        df["VAR_CAP"] = np.where((df['CAP'] > df['QTTOTPAL']) & (df['STATUS_PROD'] != "INT"), "DIVERGENCIA", "NORMAL")
        df['VAR_ABST'] =    np.where((df['FLEG_ABST'] == 'SIM') & (df['STATUS_PROD'] == "INT"), "NORMAL",
                            np.where((df['FLEG_ABST']== 'NÃO') & (df['STATUS_PROD'] == "DIV"), "NORMAL",
                            "DIVERGENCIA"))

        df['VAR_CUBAGEM'] = np.where(df['volume_master'] < df['volume_venda'], "DIVERGENCIA", "NORMAL")   
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 3: {error}\n")

    try:# ETAPA 4: GERANDO O ARQUIVO FINAL
        drop_col = ['PKESTRU', 'PK_END', 'CARACTERISTICA', 'PULMAO','TIPO_1', 'PRAZOVAL', 'PERCTOLERANCIAVAL', 'DTULTENT','USAWMS', 'REVENDA', 'TIPOPROD','CODFORNEC', 'FORNECEDOR', 'TIPO', 'PONTOREPOSICAO','CODAUXILIAR', 'EMBALAGEM', 'EMBALAGEMMASTER', 'QTUNITCX', 'CODSEC','SECAO', 'LASTROPAL', 'ALTURAPAL', 'QTTOTPAL', 'ALTURAM3', 'LARGURAM3','COMPRIMENTOM3', 'ALTURAARM', 'LARGURAARM', 'COMPRIMENTOARM','PESOBRUTOMASTER', 'PESOLIQMASTER', 'PESOBRUTO', 'PESOLIQ','OBS2', 'CODAUXILIAR2','volume_venda', 'volume_master','CODFILIAL']
        df = df.drop(columns=drop_col)

        df.to_excel(output.var_cadastro, index= False, sheet_name= "DIV_CADASTRO")
        print("Arquivo salvo com sucesso...\n")
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 4: {error}\n")
    
    try:
        print("=" * 24 + "DEMOSTRATIVO" + "=" * 24)

        var_cap = df.groupby("VAR_CAP").agg(
            TT_PROD = ('CODPROD', 'nunique')
        ).reset_index()

        var_abst = df.groupby("VAR_ABST").agg(
            TT_PROD = ('CODPROD', 'nunique')
        ).reset_index()

        var_cubagem = df.groupby("VAR_CUBAGEM").agg(
            TT_PROD = ('CODPROD', 'nunique')
        ).reset_index()

        print(f"\n{var_cap}\n")
        print("=" * 60)
        print(f"\n{var_abst}\n")
        print("=" * 60)
        print(f"\n{var_cubagem}\n")
        print("=" * 60)
    except Exception as e:
        error = validar_erro(e)
        print(f"ETAPA 5: {error}\n")
if __name__ == "__main__":
    app = app()
    input("\nPressione Enter para sair...")