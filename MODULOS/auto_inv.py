import pyautogui as pag
import pyperclip as pc
from tkinter import messagebox
import tkinter as tk
import threading
import queue
import time 

class AUTO_INV:
    def __init__(self):
        self.root = tk.Toplevel()
        self.root.title("Automação Inventario")
        self.root.configure(bg="#0C316B")
        self.root.geometry("330x300")
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
                    self.observacao.config(text="Aguardando códigos...")
                    self.text_codprod.delete("1.0", tk.END)

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
            if not self.lista_codprod:
                self.fila.put(("erro", "As listas de códigos estão vazias."))
                return
                
            pag.keyDown('alt')
            pag.press('tab')
            pag.keyUp('alt')
            time.sleep(0.02)
            
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
            for i, (codprod) in enumerate(self.lista_codprod, 1):
                if not self.em_execucao:
                    break
                
                pc.copy(codprod)
                pag.hotkey("ctrl", "v")
                pag.press('enter')
                time.sleep(0.0)

                for _ in range(4):
                    pag.press('tab')
                    time.sleep(0.0)

                pag.press('enter')

                for _ in range(3):
                    pag.press('tab')
                    time.sleep(0.0)

                pag.press('enter')
                pag.press('tab')

                self.fila.put(("contador", (i, total_prod)))
                print(f"Processado: {i}/{total_prod}")
                
            if self.em_execucao:  # Só mostra se não foi cancelado
                self.fila.put(("concluido", "Transferência concluída com sucesso!"))

                
        except Exception as e:
            self.fila.put(("erro", f"Ocorreu um erro: {str(e)}"))

    def interface(self):
        self.observacao = tk.Label(self.root, text="Aguardando códigos...", bg="#0C316B", fg="#FFFFFF")
        self.observacao.place(relx=0.050, rely=0.005, relwidth=0.900, relheight=0.200)

        self.text_codprod = tk.Text(self.root, height=10, width=40,bd= 2, bg="#FFFFFF", fg="#000000")
        self.text_codprod.place(relx= 0.050, rely= 0.250, relwidth=0.900, relheight=0.500)

        self.contador = tk.Label(self.root, text="Contador: 0/0", bg="#0C316B", fg="#FFFFFF")
        self.contador.place(relx=0.050, rely=0.770, relwidth=0.900, relheight=0.050)

        self.btn_iniciar = tk.Button(self.root, text="Iniciar Transferência", bg="#FFFFFF", fg="#000000",command= self.BFF)
        self.btn_iniciar.place(relx=0.050, rely=0.870, relwidth=0.900, relheight=0.100)
