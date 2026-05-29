import tkinter as tk


class PainelLog:
    def __init__(self, mapa):
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 16, "bold"), "justify": "center"}

        self.list_check = []

        self.estados = {}
        self.mapa_relacao = {}
        self.path = {}

        root = tk.Toplevel()
        root.title("GERENCIADOR_8000")
        root.geometry("400x300")
        root.resizable(False,False)
        root.config(bg= self.background)
        # root.iconbitmap(Assets.IcoPrincipal)
        chaves = mapa.keys()

        for var in chaves:
            self.mapa_relacao[var] = mapa[var]["nomeclatura"]
            self.path[var] = mapa[var]["path"]
            self.estados[var] = tk.BooleanVar(value=False)  

        self.componentes(root)
        self.Clicaveis(root)
        self.Localizar()
        
        # root.mainloop()
        pass
    def _start_UI(self):
        selecionados = [nome for nome, var in self.estados.items() if var.get()]

        print(selecionados)
        pass
    
    def componentes(self, janela_principal):
        self.back = tk.Frame(
            janela_principal
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        """Criar o log de estagio indentificar oque deu erro"""
        
        for chave_interna, texto_exibicao in self.mapa_relacao.items():
            var_estado = self.estados.get(chave_interna)
            check = tk.Checkbutton(
                self.back,
                text=texto_exibicao,
                variable=var_estado,
                font= ("verdana", 9,"bold"),
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
    def Clicaveis(self, janela_principal):
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
            ,command=lambda: self._start_UI()
        )

        pass
    def Localizar(self):
        self.back.place(relx= 0.01, rely= 0.01, relheight= 0.50, relwidth= 0.98)
        self.bt_iniciar.place(relx= 0.01, rely= 0.84, relheight= 0.15, relwidth= 0.98)
        
        _relx_T1 = 0.01
        _relx_T2 = 0.53

        _rely_inicial = 0.03
        _altura_item = 0.15
        total = round(len(self.list_check) / 2)
        for indice, item_check in enumerate(self.list_check):
            if indice < total:
                _relx = _relx_T1
                posicao_y = _rely_inicial + (indice * _altura_item)

            else:
                _relx = _relx_T2
                posicao_y = _rely_inicial + ((indice-total) * _altura_item)

            item_check.place(
                relx=_relx
                ,rely=posicao_y
                ,relheight=_altura_item
            )    
        pass
