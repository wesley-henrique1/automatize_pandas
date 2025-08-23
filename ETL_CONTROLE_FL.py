from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np

col_96 = ['CODPROD', 'DESCRICAO','OBS2', 'RUA', 'PREDIO', 'APTO', ]
col_286 = ['Código', 'Estoque', 'Qtde Pedida', 'Bloqueado(Qt.Bloq.-Qt.Avaria)', 'Qt.Avaria', 'Custo real', 'Disponível']
df_8596 = pd.read_excel(ar_xlsx.ar_96, usecols= col_96)
df_286 = pd.read_excel(ar_xls.ar_86, usecols= col_286)
final = [105,106,107,108,64,63,61,62]

def app():
    print("=" * 60)
    print("iniciando processo...")
    df = df_8596.merge(df_286, left_on= 'CODPROD', right_on= 'Código', how= 'inner')
    df = df[df['RUA'].between(1, 40)]
    df['BLOQ + AV'] = df['Bloqueado(Qt.Bloq.-Qt.Avaria)'] + df['Qt.Avaria']
    df['BLOQ?'] = np.where(df['BLOQ + AV'] >= df['Estoque'], 'S', 'N')
    df['situação'] = np.where(df['PREDIO'].isin(final), "S","N")
    df['total_custo'] = df['Custo real'] * df['Disponível']

    fora = ["Código", "Bloqueado(Qt.Bloq.-Qt.Avaria)", "Qt.Avaria"]
    df.drop(fora, axis= 1, inplace= True)
    
    df_os_esquecidos = df.loc[(df['OBS2'] == 'FL') & (df['Estoque'] > 0) & (df['BLOQ?'] == "N")]
    df_fantasminha = df.loc[(df['OBS2'] == 'FL') & (df['Estoque'] == 0)]
    df_hora_extra = df.loc[(df['OBS2'] != 'FL') & (df['Estoque'] == 0) & (df['Qtde Pedida'] == 0)]


    with pd.ExcelWriter(output.FL) as feijao:
        df_os_esquecidos.to_excel(feijao, sheet_name= "FL_ATIVOS", index= False)
        df_fantasminha.to_excel(feijao, sheet_name= "FL_END", index= False)
        df_hora_extra.to_excel(feijao, sheet_name= "INATIVOS", index= False)
        df.to_excel(feijao, sheet_name= "GERAL", index= False)
        print("=" * 60)
        print('fim do processo, verifique o arquivo controle_corte.xlsx')

if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")