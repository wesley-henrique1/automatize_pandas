from tkinter import messagebox 
import tkinter as tk

background = "#2D2D2D"
frame_color = "#2D2D2D"

text_color = "#E0E0E0"
borda_color = "#910083"


class auxiliares:
    def iniciar_processamento(self, objetivo):
        selecionados = [nome for nome, var in self.estados.items() if var.get()]
        if not selecionados:
            messagebox.showwarning("Aviso", "Selecione ao menos uma opção!")
            return
        for nome in selecionados:
            try:
                classe_do_script = self.scripts_map[nome]
                instancia = classe_do_script() 
                
                # 3. Define quais arquivos ESSE script específico precisa
                # (Você pode criar uma função que retorna a lista de paths)
                arquivos = self.obter_paths_especificos(nome)
                
                # 4. Chama o carregamento que você criou
                instancia.carregamento(arquivos)
                
                # 5. Se passou pelo carregamento, executa a lógica principal
                instancia.executar() 
                
            except Exception as e:
                messagebox.showerror("Erro Crítico", f"Falha no módulo {nome}:\n{e}")
                break # Para o processamento se um módulo falhar feio

        texto_formatado = ""
        for i, item in enumerate(selecionados, start=1):
            texto_formatado += f"{i} - {item}\n"

        if not selecionados:
            objetivo.config(text=f"Nenhuma opção selecionada.")
        else:
            objetivo.config(
                text=f"Iniciando processos para:\n{texto_formatado}"
                ,justify= "left"
                ,anchor= "nw"
            )
class Principal(auxiliares):
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
            "Abastecimento": BI_ABST,
            "Giro_estatus": Giro_Status,
            "Acuracidade": acuracidade,
            "Cadastro": cadastro,
            "cheio_vazio": cheio_vazio,
            "Corte": corte,
            "Validar_os": app,
            "Contagem": consolidado_inv
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
        self.retorno = tk.Label(
            janela_principal
            ,text="Aguardando inicialização..."
            ,font=("Verdana", 10)
            ,bg= frame_color
            ,fg= text_color
            ,highlightbackground= borda_color
            ,highlightthickness= 2
            ,justify= "center"
            ,anchor= "center"
            ,padx=10, pady=10
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
            ,command=lambda: self.iniciar_processamento(self.retorno)
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

