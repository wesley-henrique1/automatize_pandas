"""
    modulo feito para centralizar as configuração dos json

    1 - botão para alterar a jornada do dia 
    2 - botão para alterar os modulos

    para cada configuração vai ser necessário criar cria uma função para alterar cada componente

    modulos: alterar, configurar, apagar
    componentes= 
    "ChavePrincipal": {  ok
        "_chave": "Argumento",  ok
        "_Nomeclatura": "Nome da ação",
        "_Modulo": "Nome do arquivo .py",
        "_classes": "Nome da classe",
        "_estado": "tipo booleano"
    },
"""
from tkinter import messagebox, font
import tkinter as tk
import json
def TelaJornada(self):
    pass
def TelaRotinas(self):
    pass

class Modulos:
    def __init__(self, telas):
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 16, "bold"), "justify": "center"}

        self.estado_modulo = tk.BooleanVar(value=False)
        self.valida_cmd = telas.register(self._verificar_limite)
        self.fonte = font.Font(family="verdana", size= 10)

        self.TelaModulos(telas)
        self.Clicaveis()
        self.localizador()
        pass
    def _verificar_limite(self, texto_futuro, valor):
        return len(texto_futuro) <= int(valor)
    
    def TelaModulos(self, telas):
        self.tela = tk.Frame(
            telas
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )

        self.TextoKey = tk.Label(self.tela, text="Chave Principal:", anchor= 'w', font= self.fonte)
        self.CampoKey = tk.Entry(self.tela, width=30, font= self.fonte, validate= 'key', validatecommand=(self.valida_cmd, '%P', '18'))      

        self.Texto2key = tk.Label(self.tela, text="Chave Modulo:", anchor= 'w', font= self.fonte)
        self.Campo2Key = tk.Entry(self.tela, width=30, font= self.fonte, validate= 'key', validatecommand=(self.valida_cmd, '%P', '8'))        
        
        self.TextoNome = tk.Label(self.tela, text="Descrição da função:", anchor= 'w', font= self.fonte)
        self.CampoNome = tk.Entry(self.tela, width=30, font= self.fonte, validate= 'key', validatecommand=(self.valida_cmd, '%P', '27')) 

        self.TextoArquivo = tk.Label(self.tela, text="Nome do Arquivo .py:", anchor= 'w', font= self.fonte)
        self.Campoarq = tk.Entry(self.tela, width=30, font= self.fonte, validate= 'key', validatecommand=(self.valida_cmd, '%P', '14'))

        self.TextoModulo = tk.Label(self.tela, text="Nome da classe:", anchor= 'w', font= self.fonte)
        self.CampoMod = tk.Entry(self.tela, width=30, font= self.fonte, validate= 'key', validatecommand=(self.valida_cmd, '%P', '10'))

        self.TextoBool = tk.Label(self.tela, text="Estado do modulo:", anchor= 'w', font= self.fonte)
        self.booleano = tk.Checkbutton(
            self.tela
            ,variable=self.estado_modulo
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,font=self.fonte
            ,padx= 0.01
            ,pady= 0.01
        )
        pass
    def Clicaveis(self):
        self.BTCriar = tk.Button(
            self.tela
            ,text= "Adicionar"
            ,cursor= "hand2"
            ,relief="solid"
            ,font=self.fonte
            ,bg=self.borda_color
            ,fg=self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
            ,command= None
        )
        self.BTAlterar = tk.Button(
            self.tela
            ,text= "Alterar"
            ,cursor= "hand2"
            ,relief="solid"
            ,font=self.fonte
            ,bg=self.borda_color
            ,fg=self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
            ,command= None
        )
        self.BTExcluir = tk.Button(
            self.tela
            ,text= "Excluir"
            ,cursor= "hand2"
            ,relief="solid"
            ,font=self.fonte
            ,bg=self.borda_color
            ,fg=self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
            ,command= None
        )
    def localizador(self):
        self.tela.place(relx= 0.01, rely= 0.01, relheight= 0.98, relwidth= 0.98)

        self.BTCriar.place(relx= 0.01, rely= 0.79, relheight= 0.15, relwidth= 0.20)
        self.BTAlterar.place(relx= 0.22, rely= 0.79, relheight= 0.15, relwidth= 0.20)
        self.BTExcluir.place(relx= 0.44, rely= 0.79, relheight= 0.15, relwidth= 0.20)

        self.TextoKey.place(relx= 0.01, rely= 0.05, relheight= 0.10, relwidth= 0.25)
        self.CampoKey.place(relx= 0.25, rely= 0.05, relheight= 0.10, relwidth= 0.30)

        self.Texto2key.place(relx= 0.57, rely= 0.05, relheight= 0.10, relwidth= 0.22)
        self.Campo2Key.place(relx= 0.79, rely= 0.05, relheight= 0.10, relwidth= 0.19)

        self.TextoNome.place(relx= 0.01, rely= 0.20, relheight= 0.10, relwidth= 0.32)
        self.CampoNome.place(relx= 0.32, rely= 0.20, relheight= 0.10, relwidth= 0.66)

        self.TextoArquivo.place(relx= 0.01, rely= 0.40, relheight= 0.10, relwidth= 0.33)
        self.Campoarq.place(relx= 0.33, rely= 0.40, relheight= 0.10, relwidth= 0.35)
 
        self.TextoModulo.place(relx= 0.01, rely= 0.55, relheight= 0.10, relwidth= 0.25)
        self.CampoMod.place(relx= 0.25, rely= 0.55, relheight= 0.10, relwidth= 0.35)

        self.TextoBool.place(relx= 0.62, rely= 0.55, relheight= 0.10, relwidth= 0.28)
        self.booleano.place(relx= 0.90, rely= 0.55, relheight= 0.10)
        pass

    pass
class PainelConfigurar:
    def __init__(self):
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 16, "bold"), "justify": "center"}


        root = tk.Tk()
        root.title("Painel de configuração")
        root.geometry("500x300")
        root.resizable(False,False)
        root.config(bg= self.background)
        # root.iconbitmap(Assets.IcoPrincipal)

        self.componentes(root)
        self.clicaveis()
        self.Localizar()
        root.mainloop()
        pass

    def componentes(self, tela):
        self.AlaBotao = tk.Frame(
            tela
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.AlaAlterar = tk.Frame(
            tela
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
            ,padx= 2
            ,pady= 2
        )
        pass
    def clicaveis(self):
        self.BTModulos = tk.Button(
            self.AlaBotao
            ,text= "Modulos"
            ,cursor= "hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground= self.borda_color
            ,command=lambda: Modulos(self.AlaAlterar)
        )

        self.BTJornadas = tk.Button(
            self.AlaBotao
            ,text= "Jornadas"
            ,cursor= "hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground= self.borda_color
            # ,command=lambda: Modulos(self.AlaAlterar)
        )
        self.BTRotinas = tk.Button(
            self.AlaBotao
            ,text= "Rotinas"
            ,cursor= "hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground= self.borda_color
            # ,command=lambda: Modulos(self.AlaAlterar)
        )
        pass
    def Localizar(self):
        self.AlaBotao.place(relx= 0.01, rely= 0.01, relheight= 0.30, relwidth= 0.98)
        self.AlaAlterar.place(relx= 0.01, rely= 0.32, relheight= 0.65, relwidth= 0.98)

        # Loclização dos botão

        self.BTModulos.place(relx= 0.05, rely= 0.05, relheight= 0.20, relwidth= 0.20)
        self.BTJornadas.place(relx= 0.30, rely= 0.05, relheight= 0.20, relwidth= 0.20)
        self.BTRotinas.place(relx= 0.55, rely= 0.05, relheight= 0.20, relwidth= 0.20)
        pass


if __name__ == "__main__":
    PainelConfigurar()