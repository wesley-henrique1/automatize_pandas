from OUTROS.path_arquivos import pasta, bi_abst
import pandas as pd
import numpy as pn
import glob
import os

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
def directory(files, argumento, ordem_colunas):
    directory = glob.glob(os.path.join(files, argumento))
    lista = []
    
    if not directory:
        print("AVISO: Nenhum arquivo encontrado.")
        return pd.DataFrame()

    for arquivo in directory:
        try:
            x = pd.read_excel(arquivo)
            print("-"*60)
            print(x.columns)
            print("-"*60)

            X = x[ordem_colunas]
            lista.append(x)
        except Exception as e:
            print(f"ERRO: Falha ao ler {os.path.basename(arquivo)}. Detalhe: {e}")
            continue

    if not lista:
        print("AVISO: Nenhum DataFrame válido para concatenação.")
        return pd.DataFrame()
    
    df_temp = pd.concat(lista, axis=0, ignore_index=True)
    return df_temp



def app():
    try:
        ordem = [['CODFILIAL', 'NUMOS', 'DATA', 'HORA', 'CODPROD', 'DESCRICAO','ENDERECO_ORIG', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'ENDERECO_DEST','RUA_1', 'PREDIO_1', 'NIVEL_1', 'APTO_1', 'QT', 'NUMTRANSWMS','CODROTINA', 'POSICAO', 'CODFUNCGER', 'FUNCGER', 'DTFIMOS', 'CODFUNCOS','FUNCOSFIM', 'DTESTORNO', 'CODFUNCESTORNO', 'FUNCESTORNO', 'Tipo O.S.','TIPOABAST']]
        cons28 = directory(pasta.p_28,'8628*.xlsx', ordem)

        ordem = [['TIPOOS', 'NUMBONUS', 'TIPOBONUS', 'TIPOOPER', 'STATUS', 'CODPROD','DESCRICAO', 'CODFORNEC', 'FORNECEDOR', 'DTLANC', 'DATABONUS','DTFECHAMENTO', 'CODFUNCRM', 'CONFERENTE', 'CODIGOUMA', 'NUMOS','DTINICIOOS', 'CODENDERECO', 'DTFIMOS', 'CODOPERADOR', 'OPERADOR','CODEMPILHADOR', 'EMPILHADOR', 'DATAGERACAO', 'DTVALIDADE']]
        cons64 = directory(pasta.p_64, '*8664.xlsx', ordem)

    except Exception as e:
        erros = validar_erro(e)
        print(f"EXTRAÇÃO: {erros}")

    try:
        pass
    except Exception as e:
        erros = validar_erro(e)
        print(f"TRATAMENTO: {erros}")

    try:
        cons28.to_excel(bi_abst.cons_28, index= False, sheet_name= "8628")
        cons64.to_excel(bi_abst.cons_64, index= False, sheet_name= "8664")
    except Exception as e:
        erros = validar_erro(e)
        print(f"CARGA {erros}")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para finalizar...")