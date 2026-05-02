import pyautogui as pag
import pyperclip as pc
import threading
import time

from modulos._settings import Assets
from tkinter import messagebox
import datetime as dt
import tkinter as tk

class Auxiliar():
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
            f"FONTE: Flow_Master.py | ETAPA: {etapa} | DATA: {agora}\n"
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
    
    def Parar_Processo(self):
        self.em_execucao = False

        try:
            self.bt_iniciar.config(state="normal")
            self.entry_cod.delete("1.0", tk.END)
            self.contador.config(text= self.TextLog)
        except Exception as e:
            self.validar_erro(e, "AUX: Parar_Processo") 

        pass
    def Iniciar_processo(self, _listas):
        try:
            self.em_execucao = True
            self.bt_iniciar.config(state="disabled")
            _codProd = []

            _lista = _listas.get("1.0", tk.END).strip().splitlines()
            contagem = len(_lista)

            if not _lista:
                messagebox.showerror("Aviso", "Lista esta vazia, favor informar os codigos")
                self.Parar_Processo()
                return
            for var in _lista:
                try:
                    var = int(str(var).strip())
                    _codProd.append(var)
                except (ValueError, TypeError):
                    mensagem = f"Erro de Formato: O valor '{var}' não é um código válido.\n\nUse apenas números inteiros."
                    messagebox.showwarning("Divergência de Dados", mensagem)   
                    self.Parar_Processo()
                    return

            self.em_execucao = True
            self.entry_cod.delete("1.0", tk.END)
            threading.Thread(target= self._automact, args= (_codProd,contagem,), daemon= True).start()
        except Exception as e:
            self.validar_erro(e, "AUX: Iniciar_processo") 

        pass
    def _automact(self,lista_sku, maximo):
        inicio_processo = time.time()

        lista_bool = []
        pag.keyDown('alt')
        pag.press('tab')
        pag.keyUp('alt')
        pag.sleep(0.5)
        
        for etapa, SKU in enumerate(lista_sku):            
            if not self.em_execucao:
                break
            progresso_anterior = int((etapa / maximo) * 100)
            MSG = (
                f"{f">> PROGRESSO: {etapa} - {maximo} || {progresso_anterior}%":<}\n"
                f"{f">> PRODUTO: {SKU}":<}"
            )
            self.contador.config(text= MSG)

            pc.copy(SKU)
            pag.hotkey("ctrl", "v")
            pag.press('enter')
            time.sleep(self.time_executar)

            for _ in range(4):
                if not self.em_execucao: break
                pag.press('tab')
                time.sleep(self.time_executar)
            
            if not self.em_execucao: break
            pag.press('enter')

            for _ in range(3):
                if not self.em_execucao: break
                pag.press('tab')
                time.sleep(self.time_executar)

            if not self.em_execucao: break
            pag.press('enter')
            pag.press('tab')

            progresso_atual = int(( (etapa +1) / maximo) * 100)
            MSG = (
                f"{f">> PROGRESSO: {etapa +1} - {maximo} || {progresso_atual}%":<}\n"
                f"{f">> PRODUTO: {SKU}":<}"
            )
            self.contador.config(text= MSG)

            lista_bool.append(SKU)
        
        total_bool = len(lista_bool)
        total_sku = len(lista_sku)
        fim_processo = time.time()
        tempo_total = fim_processo - inicio_processo

        if tempo_total > 60:
            minutos, segundos = divmod(tempo_total, 60)
            msg = f"{int(minutos)}min e {int(segundos)} segundos"
        else:
            msg = f"{tempo_total:.2f} segundos"    

        if total_bool == total_sku:
                messagebox.showinfo(
                    "Sucesso", 
                    f"Processado: {total_bool} de {total_sku} itens.\n"
                    f"{'—'*25}\n"
                    f"Tempo de execução {msg}."
                )
                self.bt_iniciar.config(state="normal")
        elif total_bool < total_sku:
            mensagem = (
                f"Resumo da Operação\n"
                f"{'—'*25}\n"
                f"Transferência: {total_bool} de {total_sku} SKUs\n"
                f"Último Código: {lista_bool[-1]}\n"
                f"Tempo de execução {msg}."
            )
            messagebox.showinfo("Finalizado parcialmente", mensagem)
            self.bt_iniciar.config(state="normal")
        else:
            messagebox.showerror(
                "Erro na Transferência", 
                f"Apenas {total_bool} de {total_sku} itens foram processados.\n"
                "Verifique os logs para mais detalhes."
            )
            self.bt_iniciar.config(state="normal")


        pass
class FLOW_MASTER(Auxiliar):
    def __init__(self):
        self.fonte = ("verdana", 9,"bold") 
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}
        
        self.time_executar = 0.05
        self.TextLog = (
            f"{">> PROGRESSO: 0 - 0 || 100%":<}\n"
            f"{">> PRODUTO: ______":<}"
        )

        root = tk.Tk()
        root.title("Flow_Master")
        root.geometry("300x210")
        root.resizable(False, False)
        root.config(bg=self.back_2)
        root.iconbitmap(Assets.FleshIcon)

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
            ,text= self.TextLog
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
            ,command=lambda:self.Iniciar_processo(self.entry_cod)
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
            ,command=lambda: self.Parar_Processo()
        )

        pass   
    def localizador(self):
        self.frame_fundo.place(relx= 0.02, rely= 0.01, relwidth= 0.96, relheight= 0.98)

        self.entry_cod.place(relx= 0.01, rely= 0.01, relwidth= 0.98, relheight= 0.20)
        self.contador.place(relx= 0.01, rely= 0.32, relwidth= 0.98, relheight= 0.40)

        self.bt_iniciar.place(relx= 0.01, rely= 0.78, relwidth= 0.48, relheight= 0.20)
        self.bt_parar.place(relx= 0.51, rely= 0.78, relwidth= 0.48, relheight= 0.20)

        pass
