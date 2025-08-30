from path_arquivos import *
import pandas as pd

try:
    df = pd.read_csv(ar_csv.ar_end, header= None,  usecols= [0,1,2,3,4, 6, 7, 18,19])
    df.columns = ['COD_END', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'COD', 'DESC', 'ENTREDA', 'SAIDA']
except Exception as e:
    print("algo deu erro, não sei onde")

try:   
    cheio = df.loc[df['COD'] != 0]
    cheio = cheio.loc[(cheio['ENTREDA'] == 0) & (cheio['SAIDA'] == 0)]
    vazio = df.loc[df['COD'] == 0]
except Exception as e:
    print("deu erro na etapa de tratamento")


with pd.ExcelWriter(output.cheio_vazio) as caminho:
    cheio.to_excel(caminho, index= False, sheet_name= "CHEIO")
    vazio.to_excel(caminho, index= False, sheet_name= "VAZIO")