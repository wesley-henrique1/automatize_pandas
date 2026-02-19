import sys
import os

# Adds the parent directory (TRABALHO_JC) to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modulos._settings import Path_dados
from modulos._settings import Path_dados
from tkinter import messagebox
import tkinter as tk


class UI_inv:
    def __init__(self):
        self.fonte = ("verdana", 9,"bold") 
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}

        root = tk.Tk()
        root.title("FLOW-MASTER")
        root.geometry("360x360")
        root.resizable(False, False)
        root.config(bg=self.back_2)
        root.iconbitmap(Path_dados.icone_pricipal)

        self.componentes(root)
        self.localizador()
        root.mainloop()
        pass

    def componentes(self, janela):
        self.frame_fundo = tk.Frame(
            janela
            ,bg= self.background
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.entry_cod = tk.Text(
            self.frame_fundo
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
        )

        self.bt_iniciar = tk.Button(
            self.frame_fundo
            ,text="INICIAR"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            # ,command=lambda: AUTO_3707()
        )
        self.bt_parar = tk.Button(
            self.frame_fundo
            ,text="PARAR"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            # ,command=lambda: AUTO_3707()
        )

        pass
    def localizador(self):
        self.frame_fundo.place(relx= 0.01, rely= 0.01, relwidth= 0.98, relheight= 0.98)
        self.entry_cod.place(relx= 0.01, rely= 0.01, relwidth= 0.98, relheight= 0.10)
        self.bt_iniciar.place(relx= 0.01, rely= 0.50, relwidth= 0.20, relheight= 0.10)
        self.bt_parar.place(relx= 0.22, rely= 0.50, relwidth= 0.20, relheight= 0.10)
        
        pass

if __name__ == "__main__":
    UI_inv()