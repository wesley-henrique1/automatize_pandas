from modulos._settings import Path_dados

import tkinter as tk
from tkinter import messagebox
import pyautogui as pag
import pyperclip as pc
import threading
import time
import datetime as dt

class Auxiliares:
    def validar_erro(self, e, etapa):
        largura = 78
        mapeamento = {
            PermissionError: "Arquivo aberto ou sem permissão. Feche o Excel.",
            FileNotFoundError: "Arquivo de origem não encontrado. Verifique a pasta 'base_dados'.",
            KeyError: f"Coluna ou chave não encontrada: {e}",
            TypeError: f"Incompatibilidade de tipo: {e}",
            ValueError: f"Formato de dado inválido: {e}",
            NameError: f"Variável ou função não definida: {e}"
        }
        msg = mapeamento.get(type(e), f"Erro não mapeado: {e}")
        agora = dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        log_conteudo = (
            f"{'='* largura}\n"
            f"FONTE: Flow_Feeder.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            messagebox.showwarning(
                "Aviso de Interrupção", 
                "Ocorreram erros durante o processamento.\n\n"
                "Consulte o arquivo 'log_erros.txt' para detalhes técnicos."
            ) 
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")

    def Lg_parar(self):
        try:
            self.em_execucao = False  
            self.CodProd.delete("1.0", tk.END)
            self.CodEnd.delete("1.0", tk.END)
            self.logUI.config(text=self.text_logUI)
            self.btTransferir.config(state="normal")
        except Exception as e:
            self.validar_erro(e, "AUX: Lg_parar")

        return self.em_execucao    
    def Lg_transferir(self, listCod, listEnd):
        self.em_execucao = True
        self.btTransferir.config(state="disabled")

        list_prod = listCod.get("1.0", tk.END).strip().splitlines()
        list_end = listEnd.get("1.0", tk.END).strip().splitlines()

        if not list_prod or not list_end:
            messagebox.showerror("Erro", "Uma ou ambas as listas estão vazias!")
            return
        
        _codProd = []
        _codEnd = []
        for var in list_prod:
            var = int(str(var).strip())
            _codProd.append(var)
        for var in list_end:
            var = int(str(var).strip())
            _codEnd.append(var)

        TT_PROD = len(_codProd)
        TT_end = len(_codEnd)

        if TT_PROD != TT_end:
            mensagem = f"As listas possuem quantidades diferentes!\n\nProdutos: {TT_PROD}\nEndereços: {TT_end}"
            messagebox.showwarning("Divergência", mensagem)
            return

        try:
            self.CodProd.delete("1.0", tk.END)
            self.CodEnd.delete("1.0", tk.END)
            threading.Thread(target= self._automact, args= (_codProd,_codEnd,TT_PROD,)).start()
        except Exception as e:
            self.validar_erro(e, "AUX: Lg_transferir")

    def _LogUI(self, fase, barramento, progresso, cod, end):
        MSG = (
            f"{f">> PROGRESSO: {fase} - {barramento} || {progresso}%":<}\n"
            f"{f">> PRODUTO: {cod} | {end}":<}"
        )
        self.logUI.config(text= MSG)

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
                print(f"etapa: {etapa} - {codprod} | {codend}")
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

            if total_bool == total_sku:
                messagebox.showinfo(
                    "Sucesso", 
                    f"Processado: {total_bool} de {total_sku} itens.\n"
                    f"{'—'*25}\n"
                    f"Tempo de execução {tempo_total:.1f} segundos."
                )
                self.btTransferir.config(state="normal")
            elif total_bool < total_sku:
                mensagem = (
                    f"Resumo da Operação\n"
                    f"{'—'*25}\n"
                    f"Transferência: {total_bool} de {total_sku} SKUs\n"
                    f"Último Código: {lista_bool[-1]}"
                    f"Tempo de execução {tempo_total:.1f} segundos."
                )
                messagebox.showinfo("Finalizado parcialmente", mensagem)
                self.btTransferir.config(state="normal")
            else:
                messagebox.showerror(
                    "Erro na Transferência", 
                    f"Apenas {total_bool} de {total_sku} itens foram processados.\n"
                    "Verifique os logs para mais detalhes."
                )
                self.btTransferir.config(state="normal")
        except Exception as e:
            print(f"erro: _automact\n{e}\n")
            self.validar_erro(e, "AUX: _automact") 
    pass
class FLOW_FEEDER(Auxiliares):
    def __init__(self):
        self.fonte = ("verdana", 9,"bold") 
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}

        self.text_logUI = (
            f"{">> PROGRESSO: 0 - 0 || 100%":<}\n"
            f"{">> Fase: PRODUTO | DESTINO"}"
        )
        self.time_executar = 0.05

        root = tk.Tk()
        root.title("Flow_Feeder")
        root.geometry("300x210")
        root.resizable(False, False)
        root.config(bg=self.back_2)
        root.iconbitmap(Path_dados.icone_pricipal)
        
        self.componentes(root)
        self.clicaveis()
        
        self.localizar()
        root.mainloop()
        pass

    def componentes(self, tela):
        self.tela_Segundaria = tk.Frame(
            tela
            ,bg= self.background
            ,highlightbackground= self.frame_color
            ,highlightthickness= 3
        )

        self.text_PROD = tk.Label(
            self.tela_Segundaria
            ,text= "Lista Produto:"
            ,font=("verdana", 10, "bold")
            ,bg=self.background
            ,fg=self.frame_color
            ,anchor= "nw"
            ,justify="left"
            ,padx=5
            ,pady=5
        )
        self.text_END = tk.Label(
            self.tela_Segundaria
            ,text= "Lista Destino:"
            ,font=("verdana", 10, "bold")
            ,bg=self.background
            ,fg=self.frame_color
            ,anchor= "nw"
            ,justify="left"
            ,padx=5
            ,pady=5
        )
        self.CodProd = tk.Text(
            self.tela_Segundaria
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
        )
        self.CodEnd = tk.Text(
            self.tela_Segundaria
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
        )
        
        self.logUI = tk.Label(
            self.tela_Segundaria
            ,text= self.text_logUI
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
    def clicaveis(self):
        self.btTransferir = tk.Button(
            self.tela_Segundaria
            ,text= "Transferir"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: self.Lg_transferir(listCod= self.CodProd, listEnd= self.CodEnd)
        )
        self.btParar = tk.Button(
            self.tela_Segundaria
            ,text= "Parar"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda:self.Lg_parar()
        )
        pass
    def localizar(self):
        self.tela_Segundaria.place(relx= 0.02, rely= 0.01, relwidth= 0.96, relheight= 0.98)
        
        self.text_PROD.place(relx= 0.02, rely= 0.01, relwidth= 0.40, relheight= 0.20)
        self.text_END.place(relx= 0.02, rely= 0.24, relwidth= 0.40, relheight= 0.20)
        self.CodProd.place(relx= 0.42, rely= 0.01, relwidth= 0.55, relheight= 0.20)
        self.CodEnd.place(relx= 0.42, rely= 0.24, relwidth= 0.55, relheight= 0.20)

        self.logUI.place(relx= 0.02, rely= 0.46, relwidth= 0.95, relheight= 0.25)

        self.btTransferir.place(relx=0.19, rely=0.80, relwidth=0.30, relheight=0.15)
        self.btParar.place(relx=0.51, rely=0.80, relwidth=0.30, relheight=0.15)


if __name__ == "__main__":
    FLOW_FEEDER()