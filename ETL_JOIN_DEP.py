from OUTROS.path_arquivos import *
import pandas as pd
import glob
import os

def app():
    try:
        print("=========INICIANDO PROCESSO DE LEITURA=========")
        deposito = glob.glob(os.path.join(pasta.p_82, 'DEP*.txt'))
        df_DEP = pd.DataFrame([])
        movi = glob.glob(os.path.join(pasta.p_82, 'MOV*.txt'))
        df_mov = pd.DataFrame([])
        print("\nprocesso finalizado\n")
        print("listas de arquivos:")
        print(f"depositos: {deposito}")
        print(f"movimentação: {movi}")
    except Exception as e:
        print(f"erro na operação")

    def loop_ar(ar,df):
        lista = []
        for arquivo in ar:
            x = pd.read_csv(arquivo, header= None)
            lista.append(x)

        df_final = pd.concat(lista, axis= 0, ignore_index= True)
        return df_final

    df_DEP = loop_ar(deposito, df_DEP)
    df_mov = loop_ar(movi, df_mov)

    df_DEP.columns = col_name.c82
    df_mov.columns = col_name.cMovi

    with pd.ExcelWriter(output.dep_82) as writer:
        df_DEP.to_excel(writer, engine='openpyxl', index= False, sheet_name= "depositos")
        df_mov.to_excel(writer, engine= 'openpyxl', index= False, sheet_name= "MOVI")

if __name__ == "__main__":
    app()
    print('fim do processo, verifique o arquivo')
    input("\nPressione Enter para sair...")

