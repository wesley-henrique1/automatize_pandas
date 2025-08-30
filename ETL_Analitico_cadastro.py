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
        print("ANALITICO CADASTRO")
        print("=" * 70)
        print("\n iniciando as tratativa da base de dados...")
        df_96 = pd.read_excel(ar_xlsx.ar_96, usecols= ['CODPROD', 'DESCRICAO', 'OBS2', 'QTUNITCX', 'LASTROPAL', 'ALTURAPAL', 'QTTOTPAL','ALTURAARM', 'LARGURAARM', 'COMPRIMENTOARM', 'CAPACIDADE', 'ABASTECEPALETE', 'RUA', 'PREDIO', 'NIVEL', 'APTO'])
        colunas_int = ['CODPROD','QTUNITCX', 'LASTROPAL', 'QTTOTPAL', 'ALTURAPAL', 'CAPACIDADE']
        colunas_float = [ 'ALTURAARM', 'LARGURAARM', 'COMPRIMENTOARM']
        df_96[colunas_int] = df_96[colunas_int].fillna(0).astype(int)  
        df_96[colunas_float] = df_96[colunas_float].fillna(0).astype(float) 
        df = df_96[df_96["RUA"].between(1,39)]

    except PermissionError as e:
        print(f"Arquivos aberto, favor fechar. {e}")
    try:
        df['CAP_CONVERTIDA'] = df['CAPACIDADE'] * df['QTUNITCX']
        concat = df['RUA'].astype(str) + "-" + df['PREDIO'].astype(str)
        df['CONTAGEM'] = concat.map(concat.value_counts())
        df['SITUAÇÃO_PROD'] = np.where(df['CONTAGEM'] > 3, "DIV", np.where(df['CONTAGEM'] > 2,"VAL", "INT"))
        calculo = (df['ALTURAARM'] * df['ALTURAPAL']).fillna(0).astype(int)
        x_limpo = calculo.fillna(0)
        altura_arm_limpa = df['ALTURAARM'].fillna(0)
        df['P+L?']= np.where(altura_arm_limpa == 0,0,(167 - x_limpo) / altura_arm_limpa).astype(int)

        df.to_excel(output.baixa, index= False, sheet_name= "validação_PROD")
        print("Processo finalizado com sucesso...")
    except Exception as e:
        print(f"Erro na tratativa do dataFrame 8596")

if __name__ == '__main__':
    app()
    input("\nPressione Enter para sair...")
