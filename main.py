import tkinter as tk
import datetime as dt
from tkinter import messagebox
from tkinter import scrolledtext

from modulos import (
    ProcessadorLogica
    ,Assets
    ,Abastecimento
    ,Acuracidade
    ,Giro_Status
    ,Cadastro
    ,Corte
    ,Mapa_Estoque
    ,Fefo_ABST, Fefo_curva
)
from interface import (FLOW_FEEDER, FLOW_MASTER, Demandas)

class auxiliar:   
    def _exibir_mensagem_status(self, mensagem):
        self.retorno.config(state="normal")
        self.retorno.delete("1.0", "end")
        self.retorno.insert("1.0", mensagem)
        self.retorno.tag_add("alerta", "1.0", "end")
        self.retorno.tag_config("alerta", **self.estilo_alerta)
        self.retorno.config(state="disabled")        
    def start_UI(self):
        selecionados = [nome for nome, var in self.estados.items() if var.get()]
        try:
            if not selecionados:
                messagebox.showwarning("Aviso", "Selecione ao menos uma opção!")
                return
            self.logica_UI.executar_threads(selecionados)
        except Exception as e:
            self.validar_erro(e, "Main-start_UI")
    def resetar_UI(self):
        for var in self.estados:
            self.estados[var].set(False)
        
        self._exibir_mensagem_status("Aguardando proximo processo...")
        self.retorno_db.config(state="normal")
        self.retorno_db.delete("1.0", "end")

        self.contador.config(text="PROGRESSO >> 100% || 0/0 OPERAÇÃO")
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
            f"FONTE: main.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")
    
    pass
class JanelaPrincipal(auxiliar):
    def __init__(self):
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}

        root = tk.Tk()
        root.title("GERENCIADOR_8000")
        root.geometry("1005x500")
        root.resizable(False,False)
        root.config(bg= self.background)
        root.iconbitmap(Assets.FleshIcon)
        
        self.estados = {
            "Corte": tk.BooleanVar(value=False)

            ,"Fefo_abst": tk.BooleanVar(value= False)
            ,"Fefo_Curva": tk.BooleanVar(value= False)
            ,"Mapa_end": tk.BooleanVar(value= False)

            ,"Acuracidade": tk.BooleanVar(value=False)
            ,"Validar_os": tk.BooleanVar(value=False)
            ,"Cadastro": tk.BooleanVar(value=False)
            ,"Giro_estatus": tk.BooleanVar(value=False)
            ,"cheio_vazio": tk.BooleanVar(value=False)
            ,"Abastecimento": tk.BooleanVar(value=False)
            ,"Contagem": tk.BooleanVar(value=False)
        }
        self.scripts_map = {
            "Corte": Corte

            ,"Fefo_abst": Fefo_ABST
            ,"Fefo_Curva": Fefo_curva

            ,"Mapa_end": Mapa_Estoque
            ,"Acuracidade": Acuracidade
            ,"Cadastro": Cadastro
            ,"Giro_estatus": Giro_Status
            ,"Abastecimento": Abastecimento
        }    
        self.mapa_relacao = {
            "Relatorio Corte": "Corte",
            "Relatorio Acuracidade": "Acuracidade",
            "Analitico Cadastro": "Cadastro",
            "Relatorio Giro Status": "Giro_estatus",
            "Relatorio de Contagem": "Contagem",
            "Relatorio de Abastecimento": "Abastecimento",
            "FEFO: Abastecimento": "Fefo_abst",
            "FEFO: Curva ABC": "Fefo_Curva",
            "Mapeamento dos Aéreos": "Mapa_end"
        }
        self.list_check = []

        self.quadro_fleg(janela_principal= root)
        self.quadro_bt(janela_principal= root)
        self.quadro_retorno(janela_principal= root)
        self.localizador()
        self.logica_UI = ProcessadorLogica(self)
        root.mainloop()
        pass

    def quadro_fleg(self, janela_principal):
        font = ("verdana", 9,"bold")

        self.parte_1 = tk.Frame(
            janela_principal
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )

        for texto_exibicao, chave_interna in self.mapa_relacao.items():
            var_estado = self.estados.get(chave_interna)
            check = tk.Checkbutton(
                self.parte_1,
                text=texto_exibicao,
                variable=var_estado,
                font=font,
                bg=self.frame_color,
                fg=self.borda_color,
                selectcolor=self.frame_color,
                activebackground=self.frame_color,
                activeforeground=self.borda_color,
                justify="left"
                ,anchor= 'w'
            )
            self.list_check.append(check)
        pass
    def quadro_bt(self, janela_principal):
        self.parte_2 = tk.Frame(
            janela_principal
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.bt_iniciar = tk.Button(
            self.parte_2
            ,text="INICIAR"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: self.start_UI()
        )
        self.bt_demanda = tk.Button(
            self.parte_2
            ,text= "CHECKLIST"
            ,cursor= "hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: Demandas()
        )
        self.bt_info = tk.Button(
            self.parte_2
            ,text= "ROTINAS"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: self.tela_ROTINAS()
        )
        self.bt_limpar = tk.Button(
            self.parte_2
            ,text="LIMPAR"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: self.resetar_UI()
        )
        
        self.automact = tk.Label(
            self.parte_2
            ,text= "AUTOMAÇÃO"
            ,font= ("verdana", 10, "bold")
            ,anchor= "n"
            ,bg= self.back_2
            ,fg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.bt_inv = tk.Button(
            self.automact
            ,text="Flow_Master"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: FLOW_MASTER()
        )
        self.bt_07 = tk.Button(
            self.automact
            ,text="Flow_Feeder"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: FLOW_FEEDER()
        )

        pass
    def quadro_retorno(self, janela_principal):
        self.parte_3 = tk.Frame(
            janela_principal
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.contador = tk.Label(
            self.parte_3
            ,text= "PROGRESSO >> 100% || 0/0 OPERAÇÃO"
            ,font= ("Consolas", 12)
            ,fg= self.frame_color
            ,bg= self.background
            ,anchor = "w"
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.retorno = tk.Text(
            self.parte_3
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
            ,state="disabled"
        )
        self.retorno_db = tk.Text(
            self.parte_3
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
            ,state="disabled"
        )
        self.retorno_file = tk.Text(
            self.parte_3
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
            ,state="disabled"
        )
        pass
    
    def tela_ROTINAS(self):
        # Cria a janela pop-up
        janela_info = tk.Toplevel()
        janela_info.title("ROTINAS")
        janela_info.geometry("400x300")
        janela_info.resizable(False,False)
        janela_info.configure(bg=self.background)
        janela_info.iconbitmap(Assets.FleshIcon)

        self.frame_rotina = tk.Frame(
            janela_info
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.conteudo_rotina = scrolledtext.ScrolledText(
            self.frame_rotina
            ,width=110, height=35
            ,font=("Consolas", 14)
        )
        conteudo_formatado = (
            f"{"_" * 34}\n"
            f"|{"Abastecimento: 8628, 8664.":<}\n"
            f"|{"Corte: 1767, 8041.":<}\n"
            f"|{"Cadastro: 8596.":<}\n"
            f"|{"Giro estoque: 8596, 286, 1707.":<}\n"
            f"|{"Acuracidade: 286, 8596, 1707.":<}\n"
            f"|{"Ordem de serviço: 8628, 1707.":<}\n"
            f"|{"Cheio x Vazio: sem rotinas.":<}\n"
            f"|{"Inventario: 1733.":<}\n"
            f"{"_" * 34}\n"
        )
        self.conteudo_rotina.insert(tk.INSERT, conteudo_formatado)
        self.conteudo_rotina.configure(state='disabled')

        self.frame_rotina.place(relx= 0.02, rely= 0.01, relheight= 0.96, relwidth= 0.96)
        self.conteudo_rotina.place(relx= 0.01, rely= 0.01, relheight= 0.98, relwidth= 0.98)
        pass
    def tela_CORTE(self, titulo, conteudo_formatado):
        # Cria a janela pop-up
        janela_info = tk.Toplevel()
        janela_info.title(titulo)
        janela_info.geometry("1020x500")
        janela_info.resizable(False,True)
        janela_info.configure(bg=self.back_2)
        janela_info.iconbitmap(Assets.FleshIcon)
        janela_info.attributes("-topmost", True)

        self.conteudo_corte = scrolledtext.ScrolledText(
            janela_info, 
            width=110, height=35, 
            font=("Consolas", 10),
            bg= self.back_2, 
            fg= self.frame_color
        )
        self.conteudo_corte.insert(tk.INSERT, conteudo_formatado)
        self.conteudo_corte.configure(state='disabled')

        self.conteudo_corte.place(relx= 0.01, rely= 0.01, relheight= 0.98, relwidth= 0.98)
        pass
    
    def localizador(self):
        # QUADRO 1 FLEXBOX
        self.parte_1.place(relx= 0.01, rely= 0.10, relwidth= 0.40, relheight= 0.50)

        _relx_T1 = 0.01
        _relx_T2 = 0.47

        _rely_inicial = 0.03
        _altura_item = 0.10

        for indice, item_check in enumerate(self.list_check):
            if indice < 6:
                _relx = _relx_T1
                posicao_y = _rely_inicial + (indice * _altura_item)

            else:
                _relx = _relx_T2
                posicao_y = _rely_inicial + ((indice-6) * _altura_item)

            item_check.place(
                relx=_relx
                ,rely=posicao_y
                ,relheight=_altura_item
            )    

        # QUADRO 2 BOTÃO
        self.parte_2.place(relx= 0.01, rely= 0.62, relwidth= 0.40, relheight= 0.36)

        self.bt_iniciar.place(relx=0.02, rely=0.10, relwidth=0.22, relheight=0.20)
        self.bt_demanda.place(relx=0.266, rely=0.10, relwidth=0.22, relheight=0.20)
        self.bt_info.place(relx=0.512, rely=0.10, relwidth=0.22, relheight=0.20)
        self.bt_limpar.place(relx=0.758, rely=0.10, relwidth=0.22, relheight=0.20)

        self.automact.place(relx=0.01, rely=0.34, relwidth=0.98, relheight=0.65)
        self.bt_inv.place(relx=0.01, rely=0.20, relwidth=0.30, relheight=0.30)
        self.bt_07.place(relx=0.35, rely=0.20, relwidth=0.30, relheight=0.30)

        # QUADRO 3 RETORNOS
        self.parte_3.place(relx= 0.42, rely= 0.10, relwidth= 0.57, relheight= 0.88)

        self.contador.place(relx=0.01, rely=0.01, relwidth=0.98, relheight=0.10)
        self.retorno.place(relx=0.01, rely=0.15, relwidth=0.98, relheight=0.45)
        self.retorno_db.place(relx=0.01, rely=0.61, relwidth=0.50, relheight=0.38)
        self.retorno_file.place(relx=0.52, rely=0.61, relwidth=0.47, relheight=0.38)
        pass
    pass


if __name__ == "__main__":
    JanelaPrincipal()
