from path_arquivos import *
import pandas as pd
import glob
import os

def app():
    lista_cheio = glob.glob(os.path.join(pasta.p_cheio, "cheio*.xlsx"))
    tt_cheio = len(lista_cheio)

    def processar_arquivos(lista_de_arquivos):
        lista_de_dfs = []

        for arquivo in lista_de_arquivos:
            try:
                print(f"Lendo arquivo: {os.path.basename(arquivo)}")
                x = pd.read_excel(arquivo, header=2, sheet_name='RELATORIO')
                lista_de_dfs.append(x)
            except Exception as e:
                print(f"❌ Erro ao ler o arquivo '{os.path.basename(arquivo)}': {e}")
                continue

        if lista_de_dfs:
            df_final = pd.concat(lista_de_dfs, ignore_index=True)
            return df_final
        else:
            print("Nenhum DataFrame foi lido. Retornando DataFrame vazio.")
            return pd.DataFrame()
        
    df_cheio = processar_arquivos(lista_cheio)

if __name__ == '__main__':
    app()
    print('fim do processo, verifique o arquivo')
    input("\nPressione Enter para sair...")