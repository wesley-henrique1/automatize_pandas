from tkinter import messagebox 
import tkinter as tk

background = "#0C316B"

bg_label = "#0C316B"
fg_label = "#FFFFFF"

bg_text = "#FFFFFF"
fg_text = "#000000"

class auxiliares:
    def iniciar_processamento(self, objetivo):
        selecionados = [nome for nome, var in self.estados.items() if var.get()]
        if not selecionados:
            objetivo.config(text=f"Nenhuma opção selecionada.")

            print("Nenhuma opção selecionada.")
        else:
            objetivo.config(text=f"Iniciando processos para: \n{', '.join(selecionados)}",justify= "center", anchor= "w")
            print(f"Iniciando processos para: {', '.join(selecionados)}")

class Principal(auxiliares):
    
    def __init__(self):
        root = tk.Tk()
        root.title("Tela principal")
        root.geometry("450x500")
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

        self.valor_check = tk.BooleanVar()
        self.componentes(janela_principal=root)
        self.buttons(janela_principal= root)


        root.mainloop()
        

    def componentes(self, janela_principal):
        # --- 1. CONTAINER ---
        container = tk.Frame(janela_principal, bg=fg_label)
        container.place(relx=0.01, rely=0.01, relheight=0.20, relwidth=0.98)

        # --- 2. WIDGETS ---
        self.check_abst = tk.Checkbutton(
            container, text="Abastecimento"
            ,bg= fg_label, variable=self.estados["Abastecimento"]
        )
        self.check_Giro_estatus = tk.Checkbutton(
            container, text="Relatorio Giro Status"
            ,bg= fg_label, variable=self.estados["Giro_estatus"]
        )
        self.check_acuracidade = tk.Checkbutton(
            container, text="Acuracidade"
            ,bg= fg_label, variable=self.estados["Acuracidade"]
        )
        self.check_cadastro = tk.Checkbutton(
            container, text="Cadastro"
            ,bg= fg_label, variable=self.estados["Cadastro"]
        )
        self.check_ch_vz = tk.Checkbutton(
            container, text="Relatorio cheio vazio"
            ,bg= fg_label, variable= self.estados['cheio_vazio']
        )
        self.check_corte = tk.Checkbutton(
            container, text="Relatorio corte"
            ,bg= fg_label, variable=self.estados["Corte"]
        )
        self.check_validar_os = tk.Checkbutton(
            container, text="Validar ordem de serviço"
            ,bg= fg_label, variable=self.estados["Validar_os"]
        )
        self.check_contagem = tk.Checkbutton(
            container, text="Contagem inventario"
            ,bg= fg_label, variable=self.estados["Contagem"]
        )

        self.retorno = tk.Label(
            janela_principal, bg= bg_text, text="Aguardando inicialização..."
            ,font=("Verdana", 10), fg= fg_text
            ,highlightthickness= 2, highlightbackground= fg_text
            ,justify= "center", anchor= "center"
            ,padx=10, pady=10
        )
        self.retorno.place(relx=0.01, rely=0.32, relwidth=0.98, relheight=0.65)



        # --- 3. POSICIONAMENTO ---
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

    def buttons(self, janela_principal):
        bt_iniciar = tk.Button(
            janela_principal, text= "INICIAR"
            ,cursor="hand2"
            ,fg= fg_text, bg= bg_text
            ,highlightthickness= 2, highlightbackground= bg_label
            ,command= lambda: self.iniciar_processamento(self.retorno)
        )
        bt_iniciar.place(relx=0.01, rely=0.22, relwidth=0.98, relheight=0.08)


if __name__ == "__main__":
    Principal()

