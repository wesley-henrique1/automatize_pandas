import tkinter as tk
import pyautogui as pag
import pyperclip as pc
import time 
import threading
from tkinter import messagebox
import queue

class Aplicacao:
    def __init__(self, root):
        self.root = root
        self.root.title("Transferência de Códigos")
        self.root.configure(background="#0C316B")
        self.root.geometry("330x500")
        self.root.resizable(False, False)
    
        self.em_execucao = False
        self.fila = queue.Queue()
        
        self.interface()
        self.verificar_fila()

    def verificar_fila(self):
        try:
            while True:
                msg, dados = self.fila.get_nowait()
                if msg == "contador":
                    i, total = dados
                    self.contador.config(text=f"Contador: {i}/{total}")
                elif msg == "erro":
                    messagebox.showerror("Erro", dados)
                elif msg == "concluido":
                    self.observacao.config(text=" ")
                    messagebox.showinfo("Concluído", dados)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.verificar_fila)

    def BFF(self):
        if not self.em_execucao:
            self.em_execucao = True
            threading.Thread(target=self.executar_transferencia, daemon=True).start()

    def executar_transferencia(self):
        try:
            self.lista_codprod = self.text_codprod.get("1.0", tk.END).strip().splitlines()
            self.lista_codend = self.text_codend.get("1.0", tk.END).strip().splitlines()
            
            if not self.lista_codprod or not self.lista_codend:
                self.fila.put(("erro", "As listas de códigos estão vazias."))
                return
                
            pag.keyDown('alt')
            pag.press('tab')
            pag.keyUp('alt')
            time.sleep(0.1)
            
            total_prod = len(self.lista_codprod)
            self.fila.put(("contador", (0, total_prod)))
            self.observacao.config(text=f"ATENÇÃO: \nIniciando a transferência. Total de {total_prod} códigos\n, aguarde ate a finalização...\n")
            
            self.loop_transferencia(total_prod)
            
        except Exception as e:
            self.fila.put(("erro", str(e)))
        finally:
            self.em_execucao = False

    def loop_transferencia(self, total_prod):
        try:
            for i, (codprod, codend) in enumerate(zip(self.lista_codprod, self.lista_codend), 1):
                if not self.em_execucao:
                    break
                
                pc.copy(codprod)
                pag.hotkey("ctrl", "v")
                pag.press('enter')
                time.sleep(0.05)

                pc.copy(codend)
                pag.hotkey("ctrl", "v")
                for _ in range(3):
                    pag.press('enter')
                    time.sleep(1)
                
                self.fila.put(("contador", (i, total_prod)))
                print(f"Processado: {i}/{total_prod}")
                
            if self.em_execucao:  # Só mostra se não foi cancelado
                self.fila.put(("concluido", "Transferência concluída com sucesso!"))

                
        except Exception as e:
            self.fila.put(("erro", f"Ocorreu um erro: {str(e)}"))

    def interface(self):
        """Configura a interface gráfica"""
        self.frame = tk.Frame(self.root, bg="#FAFBFC")
        self.frame.place(relx=0.005, rely=0.005, relwidth=0.99, relheight=0.99)

        self.text_codprod = tk.Text(self.frame, bg="#3888c1", fg="#ffffff", bd=2, font=("Verdana", 10))
        self.text_codprod.place(relx=0.010, rely=0.100, relwidth=0.490, relheight=0.700)

        self.text_codend = tk.Text(self.frame, bg="#3888c1", fg="#ffffff", bd=2, font=("Verdana", 10))
        self.text_codend.place(relx=0.500, rely=0.100, relwidth=0.490, relheight=0.700)

        self.botao = tk.Button(self.root, text="Transferir", bg="#3888c1", fg="#ffffff", 
                             bd=3, font=("Verdana", 10), command=self.BFF)
        self.botao.place(relx=0.050, rely=0.030, relwidth=0.250, relheight=0.050)

        self.contador = tk.Label(self.root, text="Contador: 0/0")
        self.contador.place(relx=0.650, rely=0.030, relheight=0.050)

        self.observacao = tk.Label(self.root, text="ATENÇÃO: \n", bg="#ffffff", 
                                 fg="#DE2326", font=("Verdana", 8), bd=4, anchor="nw")
        self.observacao.place(relx=0.020, rely=0.810, relwidth=0.960, relheight=0.150)


if __name__ == "__main__":
    root = tk.Tk()
    app = Aplicacao(root)
    root.mainloop()