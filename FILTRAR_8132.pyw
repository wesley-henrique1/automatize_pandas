import pandas as pd
import BASE as bs
import tkinter as tk
from tkinter import messagebox

messagebox.showinfo("AFISO", "Iniciando a filtragem dos arquivos. \n\nAguarde, isso pode levar alguns minutos.")
                 
try:
    df_orginal = pd.read_excel(bs.path.p_8132, engine='openpyxl')
    print("Arquivo lido com sucesso!")

except Exception as e:
    print(f"Erro ao ler o arquivo: {str(e)}")

try:
    fora = [0,44, 50, 60, 70, 80, 100,106 ,200, 201, 202, 203]
    df_filtrado = df_orginal[~df_orginal['RUA'].isin(fora)]
    df_filtrado.to_excel(bs.dest.destino_5,engine= "openpyxl", index=False, header=True)
    messagebox.showinfo("AVISO", "Operação concluída com sucesso! \n\n( ദ്ദി ˙ᗜ˙ )")
except Exception as e:
    messagebox.showerror("Erro", f"Ocorreu um erro ao salvar o arquivo: {e}\n\nVerifique o caminho e tente novamente.") 


