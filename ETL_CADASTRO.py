from config.config_path import *
import pandas as pd
import numpy as np
import warnings
import re
warnings.simplefilter(action='ignore', category=UserWarning)

def extrair_e_converter_peso(descricao):

    match = re.search(r'([\d\.,]+)\s*(KG|GR)', str(descricao), re.IGNORECASE)
    
    if match:
        valor_str = match.group(1).replace(',', '.')
        valor = float(valor_str)
        unidade = match.group(2).upper()
        if unidade == 'KG':
            return valor * 1000
        elif unidade == 'GR':
            return valor
           
    return None
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

class cadastro:
    try:
        base = pd.read_excel(relatorios.rel_96)
        endereco = pd.read_excel(outros.ou_end, sheet_name= 'STATUS')
    except Exception as e:
        erros = validar_erro(e)
        print(f"Extração: {erros}")

    try:
        TEMP = base.merge(endereco, left_on= 'RUA', right_on= 'RUA', how= 'inner')
        df = TEMP.loc[TEMP['RUA'].between(1,39)].copy()

        dic_raname = {
            'ABASTECEPALETE' : 'FLEG_ABST',
            'CAPACIDADE' : 'CAP'
        }
        df = df.rename(columns=dic_raname)
        concat = df['RUA'].astype(str) + " - " + df['PREDIO'].astype(str)

        df['extrator'] = df['EMBALAGEMMASTER'].str.extract(r'(\d+)').astype(int)
        df['volume_master'] = df['ALTURAARM'].astype(float) * df['LARGURAARM'].astype(float) * df['COMPRIMENTOARM'].astype(float) 
        df['volume_venda'] = (df['ALTURAM3'].astype(float) * df['LARGURAM3'].astype(float) * df['COMPRIMENTOM3'].astype(float)) * df['extrator']
        df['CONT_AP'] = concat.map(concat.value_counts())

        list_int = ['2-INTEIRO(1,90)', '1-INTEIRO (2,55)']
        df['STATUS_PROD'] = np.where((df['CONT_AP'] <= 2) & (df['PK_END'].isin(list_int)), "INT", 
            np.where(df['CONT_AP'] > 3,"DIV", "VAL"))

        df['GRAMATURA_GR'] = df["DESCRICAO"].apply(extrair_e_converter_peso).fillna(0)
        chekout = [13, 27, 28, 29, 31, 32, 33, 34, 35, 36, 37, 38, 39, 44]

        try:
            df["VAL_CAP"] = np.where((df['CAP'] < df['QTTOTPAL']) & (df['STATUS_PROD'] == "INT"), "DIVERGENCIA",
                                    np.where((df['CAP'] > df['QTTOTPAL']) & (df['STATUS_PROD'] == "DIV"),"DIVERGENCIA", "NORMAL"))
            df['VAL_FLEG'] = np.where((df['FLEG_ABST'] == 'SIM') & (df['STATUS_PROD'] == "INT"), "NORMAL", 
                                    np.where((df['FLEG_ABST']== 'NÃO') & (df['STATUS_PROD'] == "DIV"), "NORMAL", "DIVERGENCIA"))
            df['VAL_CUBAGEM'] = np.where(df['volume_master'] < df['volume_venda'], "DIVERGENCIA", "NORMAL")
            df['VAL_CARACT'] = np.where(df['CARACTERISTICA'] != df['CARACT'], "DIVERGENCIA","NORMAL")
            df['VAL_PESO'] = np.where((df["GRAMATURA_GR"] >= 1000) & (df['APTO'] > 200) & (~df['RUA'].isin([31,32])), "ACIMA DA BANDEJA", "NORMAL")
            df['VAL_TIPO'] = np.where((df['TIPO_1'] == '1 - GRANDEZA') & (df['RUA'].isin(chekout)), "DIVERGENCIA", "NORMAL")

            df['VAL_PROD'] = np.where(
                (df['TIPO_y'] == 'UN') & (df['QTUNITCX'] == 1), "DIVERGENCIA",
                np.where((df['TIPO_y'] == 'CX') & (df['QTUNITCX'] != 1), "DIVERGENCIA", "NORMAL")
            )
        except Exception as e:
            erros = validar_erro(e)
            print(f"Validação: {erros}")

        try:
            drop_columns = ['CODFILIAL', 'DTULTENT','CODAUXILIAR2','CODAUXILIAR','PK_END', 'PKESTRU', 'PULMAO','PESOBRUTO', 'PESOLIQ','PESOBRUTOMASTER', 'PESOLIQMASTER', 'CODFORNEC', 'FORNECEDOR', 'CODSEC', 'SECAO', 'PRAZOVAL', 'PERCTOLERANCIAVAL','REVENDA', 'USAWMS','LASTROPAL', 'ALTURAPAL','ALTURAM3', 'LARGURAM3', 'COMPRIMENTOM3', 'ALTURAARM', 'LARGURAARM', 'COMPRIMENTOARM', 'TIPO_1', 'TIPO_NORMA', 'TIPOPROD','EMBALAGEM','EMBALAGEMMASTER','extrator','volume_master', 'volume_venda','TIPO_x', 'NIVEL','CARACTERISTICA','GRAMATURA_GR']

            df_final = df.drop(columns= drop_columns)

            ordem_primaria = ['CODPROD', 'DESCRICAO','OBS2', 'RUA', 'PREDIO', 'APTO', 'TIPO_y','CARACT']
            capacidade = ['QTUNITCX','QTTOTPAL','CAP', 'PONTOREPOSICAO','CONT_AP','FLEG_ABST','STATUS_PROD']
            validar = ['VAL_CAP', 'VAL_FLEG', 'VAL_CUBAGEM', 'VAL_CARACT', 'VAL_PESO', 'VAL_TIPO', 'VAL_PROD']
            ordem_completa = ordem_primaria + capacidade + validar
            df_final = df_final[ordem_completa]
        except Exception as e:
            erros = validar_erro(e)
            print(f"ordenação: {erros}")
    except Exception as e:
        erros = validar_erro(e)
        print(f"Tratamento: {erros}")

    try:
        df_final = df_final.sort_values(by=['RUA', 'PREDIO'], ascending= True)
        df_final.to_excel(output.cadastro, sheet_name= "cadastro", index= False)
    except Exception as e:
        erros = validar_erro(e)
        print(f"Carga: {erros}")

    try:
        print("=" * 36)
        contagem = df_final['RUA'].nunique()
        misto = df_final['RUA'].loc[df_final['TIPO_y'] == 'MISTO'].nunique()
        caixa = df_final['RUA'].loc[df_final['TIPO_y'] == 'CX'].nunique()
        unitario = df_final['RUA'].loc[df_final['TIPO_y'] == 'UN'].nunique()
        caso_1 = df_final[ordem_completa]

        print(F"{"COMPARATIVO RUA":_^33}")
        print("_" * 33)
        print(f"{"CONTAGEM":<10}|{"MISTO":<6}|{"CAIXA":<6}|{"UNITARIO":<8}")
        print(f"{contagem:^10}|{misto:^6}|{caixa:^6}|{unitario:^8}")
        print("_" * 33)


        pront_tt = df_final['CODPROD'].nunique()
        prod_caixa = df_final['CODPROD'].loc[df_final['QTUNITCX'] == 1].nunique()
        prod_unitario = df_final['CODPROD'].loc[df_final['QTUNITCX'] != 1].nunique()
        porcent_cx = round((prod_caixa / pront_tt) * 100, 2)
        porcent_un = round((prod_unitario / pront_tt) * 100, 2)

        print(F"\n{"COMPARATIVO PROD":_^35}")
        print("_" * 35)
        print(f"{"":^4}|{"CONTAGEM":^10}|{"CAIXA":^8}|{"UNITARIO":^10}")
        print(f"{"qtde":^4}|{pront_tt:^10}|{prod_caixa:^8}|{prod_unitario:^10}")
        print(f"{"%":^4}|{100:^10}|{porcent_cx:^8}|{porcent_un:^10}")
        print("_" * 35)
    except Exception as e:
        erros = validar_erro(e)
        print(f"Apresentação: {erros}")

if __name__ == "__main__":
    cadastro()
    input("Precione a tecla 'enter'...")
