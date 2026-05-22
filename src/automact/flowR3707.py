from ..lib import ValidarErros
import tkinter as tk
from tkinter import messagebox
import pyautogui as pag
import pyperclip as pc
import threading
import time
from datetime import timedelta

class Flow3707:
    validador = ValidarErros(fonte="logica Flow 3707")
    def __init__(self, UI):
        self.ancora =  UI
        self.text_logUI = (
            f"{">> PROGRESSO: 0 - 0 || 100%":<}\n"
            f"{">> Fase: PRODUTO | DESTINO"}"
        )
        self.time_executar = 0.001

        self._codProd = []
        self._codEnd = []

        self.AncoraCodProd = self.ancora.CodProd
        self.AncoraCodEnd = self.ancora.CodEnd
        self.AncoraLogUI = self.ancora.logUI
        self.AncoraBtTransferir = self.ancora.btTransferir

        pass    

    def _tratar(self, list_prod, list_end):
        if not list_prod or not list_end:
            messagebox.showerror("Erro", "Uma ou ambas as listas estão vazias!")
            self.PararProcesso() 
            return

        for var in list_prod:
            try:
                var = int(str(var).strip())
                self._codProd.append(var)
            except Exception as e:
                self.validador.registrar_log(e, "Logica: ")
                self.PararProcesso() 
                return
        for var in list_end:
            try:
                var = int(str(var).strip())
                self._codEnd.append(var)
            except Exception as e:
                self.validador.registrar_log(e, "Logica: ")
                self.PararProcesso() 
                return   

        TT_PROD = len(self._codProd)
        TT_end = len(self._codEnd)

        if TT_PROD != TT_end:
            mensagem = f"As listas possuem quantidades diferentes!\n\nProdutos: {TT_PROD}\nEndereços: {TT_end}"
            messagebox.showwarning("Divergência", mensagem)
            self.PararProcesso()
            return
        return TT_PROD
    def _LogUI(self, fase, barramento, progresso, cod, end):
        MSG = (
            f"{f">> PROGRESSO: {fase} - {barramento} || {progresso}%":<}\n"
            f"{f">> PRODUTO: {cod} | {end}":<}"
        )
        self.AncoraLogUI.config(text= MSG)
        pass
    def _automact(self, listCod, listEnd, trava):
        inicio_processo = time.time()
        try:
            lista_bool = []
            pag.keyDown('alt')
            pag.press('tab')
            pag.keyUp('alt')
            pag.sleep(0.5)

            for etapa, (codprod, codend) in enumerate(zip(listCod, listEnd)):
                if not self.em_execucao:
                    break
                progresso_anterior = int((etapa / trava) * 100)
                self._LogUI(
                    fase= etapa
                    ,barramento= trava
                    ,progresso= progresso_anterior
                    ,cod= codprod
                    ,end= codend
                )

                if not self.em_execucao: break
                pc.copy(codprod)
                pag.hotkey("ctrl", "v")
                pag.press('enter')
                time.sleep(self.time_executar)
                
                if not self.em_execucao: break
                pc.copy(codend)
                pag.hotkey("ctrl", "v")
                for _ in range(3):
                    pag.press('enter')
                    time.sleep(self.time_executar)      
                if not self.em_execucao: break
                progresso_atual = int(( (etapa +1) / trava) * 100)
                self._LogUI(
                    fase= etapa + 1
                    ,barramento= trava
                    ,progresso= progresso_atual
                    ,cod= codprod
                    ,end= codend
                )
                if not self.em_execucao: break
                lista_bool.append(codprod)

            total_bool = len(lista_bool)
            total_sku = len(listCod)

            fim_processo = time.time()
            tempo_total = fim_processo - inicio_processo
           
            msg = str(timedelta(seconds=int(tempo_total)))
            if total_bool == total_sku:
                messagebox.showinfo(
                    "Sucesso", 
                    f"Processado: {total_bool} de {total_sku} itens.\n"
                    f"{'—'*25}\n"
                    f"Tempo de execução {msg}."
                )
                self.AncoraBtTransferir.config(state="normal")
            elif total_bool < total_sku:
                mensagem = (
                    f"Resumo da Operação\n"
                    f"{'—'*25}\n"
                    f"Transferência: {total_bool} de {total_sku} SKUs\n"
                    f"Último Código: {lista_bool[-1]}\n"
                    f"Tempo de execução {msg}."
                )
                messagebox.showinfo("Finalizado parcialmente", mensagem)
                self.AncoraBtTransferir.config(state="normal")
            else:
                messagebox.showerror(
                    "Erro na Transferência", 
                    f"Apenas {total_bool} de {total_sku} itens foram processados.\n"
                    "Verifique os logs para mais detalhes."
                )
                self.AncoraBtTransferir.config(state="normal")
        except Exception as e:
            self.validador.registrar_log(e, "Logica: _automact") 
        pass
    
    def PararProcesso(self):
        try:
            self.em_execucao = False  
            self.AncoraCodProd.delete("1.0", tk.END)
            self.AncoraCodEnd.delete("1.0", tk.END)
            self.AncoraBtTransferir.config(state="normal")
            self.AncoraLogUI.config(text=self.text_logUI)
        except Exception as e:
           self.validador.registrar_log(e, "Logica: PararProcesso")

        return self.em_execucao    
        pass
    def IniciarProcesso(self, listCod, listEnd):
        self.em_execucao = True
        list_prod = listCod.get("1.0", tk.END).strip().splitlines()
        list_end = listEnd.get("1.0", tk.END).strip().splitlines()

        self.AncoraBtTransferir.config(state="disabled")
        self.AncoraCodProd.delete("1.0", tk.END)
        self.AncoraCodEnd.delete("1.0", tk.END)
        self.AncoraLogUI.config(text=self.text_logUI)

        TT_PROD = self._tratar(list_prod= list_prod, list_end= list_end)
        
        try:
            self.AncoraCodProd.delete("1.0", tk.END)
            self.AncoraCodEnd.delete("1.0", tk.END)
            threading.Thread(target= self._automact, args= (self._codProd,self._codEnd,TT_PROD,)).start()
        except Exception as e:
           self.validador.registrar_log(e, "Logica: IniciarProcesso")

