import tkinter as tk
import datetime as dt
from tkinter import messagebox
from tkinter import scrolledtext

from modulos._logic_UI import ProcessadorLogica
from modulos._settings import Path_dados

from modulos.abastecimento import Abastecimento
from modulos.acuracidade import Acuracidade
from modulos.cont_prod import Contagem_INV
from modulos.giro_st import Giro_Status
from modulos.ch_vz import Cheio_Vazio
from modulos.os_check import Os_check
from modulos.cadastro import Cadastro
from modulos.corte import Corte

class JanelaPrincipal:
    def __init__(self):
        root = tk.Tk()
        self.logica_UI = ProcessadorLogica(self)
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"

        self.borda_color = "#000000"

        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}


        root.title("Tela principal")
        root.geometry("580x500")
        root.resizable(False,True)
        root.config(bg= self.background)
        root.iconbitmap(Path_dados.icone_pricipal)
        
        self.estados = {
            "Corte": tk.BooleanVar(value=False),
            "Acuracidade": tk.BooleanVar(value=False),
            "Validar_os": tk.BooleanVar(value=False),
            "Cadastro": tk.BooleanVar(value=False),
            "Giro_estatus": tk.BooleanVar(value=False),
            "cheio_vazio": tk.BooleanVar(value=False),
            "Abastecimento": tk.BooleanVar(value=False),
            "Contagem": tk.BooleanVar(value=False)
        }
        self.scripts_map = {
            "Corte": Corte
            ,"Validar_os": Os_check
            ,"Acuracidade": Acuracidade
            ,"Cadastro": Cadastro
            ,"Giro_estatus": Giro_Status
            ,"cheio_vazio": Cheio_Vazio
            ,"Contagem": Contagem_INV
            ,"Abastecimento": Abastecimento
        }
        
        self.componentes(janela_principal= root)
        self.botoes_layout(janela_principal= root)
        self.localizador()
        root.mainloop()

    def componentes(self, janela_principal):
        # --- 1. CONTAINER ---
        self.container = tk.Frame(
            janela_principal
            ,bg= self.frame_color
            ,highlightbackground=self.borda_color 
            ,highlightthickness=3              
        )
        # --- 2. WIDGETS ---
        font = ("verdana", 9,"bold")
        self.check_abst = tk.Checkbutton(
            self.container
            ,text="Abastecimento"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable=self.estados["Abastecimento"]

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )
        self.check_Giro_estatus = tk.Checkbutton(
            self.container
            ,text="Relatorio Giro Status"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable=self.estados["Giro_estatus"]

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )
        self.check_acuracidade = tk.Checkbutton(
            self.container
            ,text="Acuracidade"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable=self.estados["Acuracidade"]

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )
        self.check_cadastro = tk.Checkbutton(
            self.container
            ,text="Cadastro"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable=self.estados["Cadastro"]

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )
        self.check_ch_vz = tk.Checkbutton(
            self.container
            ,text="Relatorio cheio vazio"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable= self.estados['cheio_vazio']

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )
        self.check_corte = tk.Checkbutton(
            self.container
            ,text="Relatorio Corte"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable=self.estados["Corte"]

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )
        self.check_validar_os = tk.Checkbutton(
            self.container
            ,text="Ordem de serviço"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable=self.estados["Validar_os"]

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )
        self.check_contagem = tk.Checkbutton(
            self.container
            ,text="Contagem inventario"
            ,font= font
            ,bg= self.frame_color
            ,fg= self.borda_color
            ,variable=self.estados["Contagem"]

            ,selectcolor=self.frame_color
            ,activebackground=self.frame_color
            ,activeforeground=self.borda_color
        )

        # --- 3. WIDGETS RETORNO---
        self.retorno = tk.Text(
            janela_principal
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
            ,state="disabled"
        )
        self.text_contador = tk.Label(
            janela_principal
            ,text= "PROGRESSO >>"
            ,font= ("Consolas", 14)
            ,fg= self.frame_color
            ,bg= self.background
            ,anchor = "w"
        )
        self.contador = tk.Label(
            janela_principal
            ,text= "100%"
            ,font= ("Consolas", 12)
            ,fg= self.frame_color
            ,bg= self.background
            ,anchor = "w"
        )
    def botoes_layout(self, janela_principal):
        self.bt_iniciar = tk.Button(
            janela_principal
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
        self.bt_limpar = tk.Button(
            janela_principal
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
    def localizador(self):
        self.container.place(relx=0.01, rely=0.01, relheight=0.20, relwidth=0.98)

        # Coluna 1
        self.check_abst.place(relx=0.02, rely=0.05)
        self.check_Giro_estatus.place(relx=0.02, rely=0.35)
        self.check_acuracidade.place(relx=0.02, rely=0.65)

        # Coluna 2
        self.check_cadastro.place(relx=0.35, rely=0.05)    
        self.check_ch_vz.place(relx=0.35, rely=0.35)  
        self.check_corte.place(relx=0.35, rely=0.65)

        # Coluna 3
        self.check_validar_os.place(relx=0.68, rely=0.05)
        self.check_contagem.place(relx=0.68, rely=0.35)

        # Retorno dos scripts
        self.retorno.place(relx=0.01, rely=0.32, relwidth=0.98, relheight=0.65)
        self.text_contador.place(relx=0.01, rely=0.22, relwidth=0.23, relheight=0.08)
        self.contador.place(relx=0.24, rely=0.22, relwidth=0.40, relheight=0.08)

        # botão
        self.bt_iniciar.place(relx=0.67, rely=0.22, relwidth=0.15, relheight=0.08)
        self.bt_limpar.place(relx=0.84, rely=0.22, relwidth=0.15, relheight=0.08)
    def segunda_tela(self, titulo, conteudo_formatado):
        # Cria a janela pop-up
        janela_info = tk.Toplevel()
        janela_info.title(titulo)
        janela_info.geometry("1020x500")
        janela_info.resizable(False,True)
        janela_info.configure(bg=self.back_2)
        janela_info.iconbitmap(Path_dados.icone_pricipal)
        janela_info.attributes("-topmost", True)

        txt_area = scrolledtext.ScrolledText(
            janela_info, 
            width=110, height=35, 
            font=("Consolas", 10),
            bg= self.back_2, 
            fg= self.frame_color
        )
        
        txt_area.insert(tk.INSERT, conteudo_formatado)
        txt_area.configure(state='disabled') # Somente leitura
        txt_area.pack(padx=10, pady=10, expand=True, fill="both")
    
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
        self.contador.config(text="100%")
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

if __name__ == "__main__":
    JanelaPrincipal()
