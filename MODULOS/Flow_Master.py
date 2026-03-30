import sys
import os

diretorio_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(diretorio_raiz)


import pyautogui as pag
import pyperclip as pc
import threading
import time

from modulos._settings import Path_dados
from tkinter import messagebox
import tkinter as tk

class Auxiliar():
    def __init__(self):
        
        self.em_execucao = False
        pass

    def iniciar(self, _listas):
        self._lista = _listas.get("1.0", tk.END).strip().splitlines()

        if not self._lista:
            messagebox.showerror("Aviso", "Lista esta vazia, favor informar os codigos")
            return
        self.em_execucao = True
        threading.Thread(target= self.musculo, daemon= True).start()

        pass
    def Parar(self):
        self.em_execucao = False

        pass
    def musculo(self):
        pag.keyDown('alt')
        pag.press('tab')
        pag.keyUp('alt')
        pag.sleep(0.5)

        for i, SKU in enumerate(self._lista):
            if not self.em_execucao:
                break

            pc.copy(SKU)
            pag.hotkey("ctrl", "v")
            pag.press('enter')
            time.sleep(0.5)

            for _ in range(4):
                if not self.em_execucao: break
                pag.press('tab')
                time.sleep(0.5)
            
            if not self.em_execucao: break
            pag.press('enter')

            for _ in range(3):
                if not self.em_execucao: break
                pag.press('tab')
                time.sleep(0.5)

            if not self.em_execucao: break
            pag.press('enter')
            pag.press('tab')


        pass
class FLOW_MASTER(Auxiliar):
    def __init__(self):
        self.fonte = ("verdana", 9,"bold") 
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}

        root = tk.Tk()
        root.title("FLOW-MASTER")
        root.geometry("260x200")
        root.resizable(False, False)
        root.config(bg=self.back_2)
        root.iconbitmap(Path_dados.icone_pricipal)

        self.componentes(root)
        self.widgets_clicaveis()
        self.localizador()

        root.mainloop() 
    
        pass
    def acao_iniciar(self):
        self.robo = Auxiliar(self.entry_cod)
        self.robo.iniciar()

        pass
    def acao_parar(self):
        if self.robo is not None:
            self.robo.parar()
            messagebox.showwarning("Aviso", "Automação interrompida!")
        pass


    def componentes(self, janela):
        self.frame_fundo = tk.Frame(
            janela
            ,bg= self.background
            ,highlightbackground= self.frame_color
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
        self.contador = tk.Label(
            self.frame_fundo
            ,text= (f"{">> PROGRESSO: 0 - 0 || 0%":<}\n"
                    f"{">> PRODUTO: ______":<}")
            ,font= ("verdana", 10, "bold")
            ,bg= self.back_2
            ,fg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
            ,anchor= "nw"
            ,justify="left"
            ,padx=5
            ,pady=5
        )
        
        pass
    def widgets_clicaveis(self): 
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
            ,command=lambda:self.iniciar(self.entry_cod)
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
            ,command=lambda: self.Parar()
        )

        pass
    
    def localizador(self):
        self.frame_fundo.place(relx= 0.02, rely= 0.01, relwidth= 0.96, relheight= 0.98)

        self.entry_cod.place(relx= 0.01, rely= 0.01, relwidth= 0.98, relheight= 0.20)
        self.contador.place(relx= 0.01, rely= 0.32, relwidth= 0.98, relheight= 0.40)

        self.bt_iniciar.place(relx= 0.01, rely= 0.78, relwidth= 0.48, relheight= 0.20)
        self.bt_parar.place(relx= 0.51, rely= 0.78, relwidth= 0.48, relheight= 0.20)

        pass

if __name__ == "__main__":
    FLOW_MASTER()