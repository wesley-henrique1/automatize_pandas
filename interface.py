from tkinter import messagebox 
import tkinter as tk
import threading
import datetime as dt

from MODULOS.BI_ABST import BI_ABST
from MODULOS.BI_Giro_Status import Giro_Status
from MODULOS.ETL_ACURACIDADE import acuracidade
from MODULOS.ETL_CADASTRO import cadastro
from MODULOS.ETL_CHEIO_X_VAZIO import cheio_vazio
from MODULOS.ETL_CORTE import corte
from MODULOS.ETL_validar_os import validar_os

background = "#2D2D2D"
frame_color = "#2D2D2D"

text_color = "#E0E0E0"
borda_color = "#910083"
import datetime as dt
import threading
from tkinter import messagebox

class Auxiliares:
    def __init__(self):
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}

    
    def iniciar_processamento(self):
        try:
            selecionados = [nome for nome, var in self.estados.items() if var.get()]

            if not selecionados:
                messagebox.showwarning("Aviso", "Selecione ao menos uma opção!")
                return

            self._exibir_mensagem_status(" >>> PROCESSANDO DADOS... POR FAVOR, AGUARDE.")
            
            thread = threading.Thread(target=self.task, args=(selecionados,), daemon=True)
            thread.start()
            
        except Exception as e:
            self.validar_erro(e, "Inicializador")

    def task(self, argumento):
        lista_de_logs = []
        for nome in argumento:
            try:
                classe_do_script = self.scripts_map[nome]
                instancia = classe_do_script() 
                temp = instancia.carregamento()
                
                if isinstance(temp, list):
                    lista_de_logs.extend(temp)
                elif isinstance(temp, dict):
                    lista_de_logs.append(temp)
                    
            except Exception as e:
                self.validar_erro(e, f"Módulo: {nome}")
                self.retorno.after(0, lambda n=nome: messagebox.showerror(
                    "Erro Crítico", f"Falha no módulo {n}:\nVerifique o log_erros.txt"
                ))
                break 
        
        self.retorno.after(0, lambda: self.atualizar_log(lista_de_logs))

    def atualizar_log(self, dados_arquivos):
        try:
            dados_validos = [d for d in dados_arquivos if isinstance(d, dict)]
            
            if not dados_validos:
                self._exibir_mensagem_status(" >>> NENHUM DADO PROCESSADO")
                return

            conteudo = f"{'ID':^3} | {'ARQUIVO':^41} | {'DATA':^10} | {'HORA':^8}\n"
            conteudo += f"{'-' * 71}\n"

            for item in dados_validos:
                nome_arq = str(item.get('ARQUIVO', 'DESCONHECIDO'))
                if len(nome_arq) > 41:
                    nome_arq = nome_arq[:38] + "..."
                
                linha = (f"{item.get('CONTADOR', 0):02d}  | {nome_arq:<41} | "
                         f"{item.get('DATA', '--/--/----'):<10} | {item.get('HORAS', '--:--'):<8}\n")
                conteudo += linha

            self._escrever_no_widget(conteudo)

        except Exception as e:
            self.validar_erro(e, "Atualizar LOG")
            self._exibir_mensagem_status(" >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt")

    def _exibir_mensagem_status(self, mensagem):
        self.retorno.config(state="normal")
        self.retorno.delete("1.0", "end")
        self.retorno.insert("1.0", mensagem)
        self.retorno.tag_add("alerta", "1.0", "end")
        self.retorno.tag_config("alerta", **self.estilo_alerta)
        self.retorno.config(state="disabled")

    def _escrever_no_widget(self, texto):
        self.retorno.config(state="normal")
        self.retorno.delete("1.0", "end")
        self.retorno.insert("end", texto)
        self.retorno.config(state="disabled")
        self.retorno.see("end")


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

class Principal(Auxiliares):
    def __init__(self):
        root = tk.Tk()
        root.title("Tela principal")
        root.geometry("580x500")
        root.config(bg= background)
        root.iconbitmap(r"config\img\sloth_icon.ico")

        self.estados = {
            "Abastecimento": tk.BooleanVar(),
            "Giro_estatus": tk.BooleanVar(),
            "Acuracidade": tk.BooleanVar(),
            "Cadastro": tk.BooleanVar(),
            "cheio_vazio": tk.BooleanVar(),
            "Corte": tk.BooleanVar(),
            "Validar_os": tk.BooleanVar(),
            "Contagem": tk.BooleanVar()
        }
        self.scripts_map = {
            "Abastecimento": BI_ABST
            ,"Giro_estatus": Giro_Status
            ,"Acuracidade": acuracidade
            ,"Cadastro": cadastro
            ,"cheio_vazio": cheio_vazio
            ,"Corte": corte
            ,"Validar_os": validar_os
            # ,"Contagem": consolidado_inv
        }

        self.valor_check = tk.BooleanVar()
        self.componentes(janela_principal= root)
        self.buttons(janela_principal= root)
        self.loc()


        root.mainloop()
        

    def componentes(self, janela_principal):
        # --- 1. CONTAINER ---
        self.container = tk.Frame(
            janela_principal
            ,bg= frame_color
            ,highlightbackground=borda_color    # Seu roxo #910083
            ,highlightthickness=3               # Espessura da borda
            )

        # --- 2. WIDGETS ---
        self.check_abst = tk.Checkbutton(
            self.container
            ,text="Abastecimento"
            ,bg= frame_color
            ,fg= text_color
            ,variable=self.estados["Abastecimento"]

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )
        self.check_Giro_estatus = tk.Checkbutton(
            self.container
            ,text="Relatorio Giro Status"
            ,bg= frame_color
            ,fg= text_color
            ,variable=self.estados["Giro_estatus"]

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )
        self.check_acuracidade = tk.Checkbutton(
            self.container
            ,text="Acuracidade"
            ,bg= frame_color
            ,fg= text_color
            ,variable=self.estados["Acuracidade"]

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )
        self.check_cadastro = tk.Checkbutton(
            self.container
            ,text="Cadastro"
            ,bg= frame_color
            ,fg= text_color
            ,variable=self.estados["Cadastro"]

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )
        self.check_ch_vz = tk.Checkbutton(
            self.container
            ,text="Relatorio cheio vazio"
            ,bg= frame_color
            ,fg= text_color
            ,variable= self.estados['cheio_vazio']

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )
        self.check_corte = tk.Checkbutton(
            self.container
            ,text="Relatorio corte"
            ,bg= frame_color
            ,fg= text_color
            ,variable=self.estados["Corte"]

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )
        self.check_validar_os = tk.Checkbutton(
            self.container
            ,text="Validar ordem de serviço"
            ,bg= frame_color
            ,fg= text_color
            ,variable=self.estados["Validar_os"]

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )
        self.check_contagem = tk.Checkbutton(
            self.container
            ,text="Contagem inventario"
            ,bg= frame_color
            ,fg= text_color
            ,variable=self.estados["Contagem"]

            ,selectcolor=frame_color
            ,activebackground=frame_color
            ,activeforeground=text_color
        )

        # --- 3. WIDGETS RETORNO---
        self.retorno = tk.Text(
            janela_principal,
            font=("Consolas", 10), # Consolas é melhor para tabelas que Verdana
            bg=frame_color,
            fg=text_color,
            highlightbackground=borda_color,
            highlightthickness=2,
            padx=10, pady=10,
            state="disabled" # Impede o usuário de digitar no log
        )        
    def buttons(self, janela_principal):
        self.bt_iniciar = tk.Button(
            janela_principal
            ,text="INICIAR"
            ,cursor="hand2"
            ,bg=background
            ,fg=text_color
            ,highlightbackground=borda_color
            ,highlightthickness=3
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,command=lambda: self.iniciar_processamento()
        )
    def loc(self):
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

        # botão
        self.bt_iniciar.place(relx=0.01, rely=0.22, relwidth=0.98, relheight=0.08)


if __name__ == "__main__":
    Principal()

