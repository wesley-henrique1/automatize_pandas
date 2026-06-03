"""
    modulo feito para centralizar as configuração dos json

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

def TelaRotinas(self):
    pass

class _Auxiliares:
    def _verificar_limite(texto_futuro, valor):
        return len(texto_futuro) <= int(valor)

class Jornadas(_Auxiliares):
    def __init__(self, telas):
        """
        para fazer a exclusão pensei em usar os indice de lista onde vai registrar dentro de uma dic o indice na chave e o texto no argumento assim quando for apagar uma tarefa não precisa passar a descrição toda so digitar o codigo de referencia

        exemplo:
              0          1          2          3          4          5      
         ['TAREFA 1','TAREFA 2','TAREFA 3','TAREFA 4','TAREFA 5','TAREFA 6']

        --Para o dia da semana pensei em colocar checkbox para cada dia assim fica melhor editirar  dando a possibilidade de edição em conjuta
        componentes seria
        bloco 1: checkbox referente a os dias da semana segunda a domingo
        bloco 2: mostrativo das tarefas, contador de tarefa(mostrar quantas tarefa tem no dia)
        bloco 3: buttom para voltar, editar, novo, salvar, proximo

        """
        self.teste = {
            "5":[
                "RELATORIO: CORTE"
                ,"RELATORIO: ABASTECIMENTO"
                ,"FEFO:(Aereo X Piking 8668)"
                ,"CADASTRO: CORREÇÃO"
                ,"INVENTARIO: ESTOQUE PARADO"
            ],
        }
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 16, "bold"), "justify": "center"}

        self.semanaKeys = {
            2: "Segunda",
            3: "Terça",
            4: "Quarta",
            5: "Quinta",
            6: "Sexta",
            7: "Sabado",
            1: "Domingo"
        }
        self.dia_atual = 1

        self.Listcheck = []
        self.variaveis_dias = {}

        self.valida_cmd = telas.register(_Auxiliares._verificar_limite)
        self.fonte = font.Font(family="verdana", size= 10)

        self.Componentes(telas)
        self.clicaveis()
        self.Localizar()

        pass
    def _mensagem(self, var):

        pass
    def _logica(self, var):
        maximo = max(self.semanaKeys)
        minimo = min(self.semanaKeys)
        self.textSemana.config(state= "normal")
        self.textSemana.delete("1.0", "end")

        if var == 1 and self.dia_atual < maximo:
            self.dia_atual += 1
            self.logSemana.config(text=self.semanaKeys[self.dia_atual])
            self.textSemana.insert("1.0", self.teste[str(self.dia_atual)])

        elif var == 0 and self.dia_atual > minimo:
            self.dia_atual -= 1
            self.logSemana.config(text=self.semanaKeys[self.dia_atual])
            self.textSemana.insert("1.0", self.teste[str(self.dia_atual)])

        else:
            self.logSemana.config(text=self.semanaKeys[self.dia_atual])

        self.textSemana.config(state= "disabled")

        pass    
    

    def Componentes(self, tela):
        self.tela = tk.Frame(
            tela
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.logSemana = tk.Label(
            self.tela
            ,text=self.semanaKeys[self.dia_atual]
            ,font=("verdana", 10, "bold")
            ,anchor="n"
            ,bg=self.back_2
            ,fg=self.frame_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=3
        )
        self.textSemana = tk.Text(
            self.tela
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=5, pady=5
            ,state="disabled"
        )
        pass
    def clicaveis(self):
        self.BTanterior = tk.Button(
            self.tela
            ,text="<<"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            ,padx= 0
            ,pady= 0
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: self._logica(0)
        )
        self.BtProximo = tk.Button(
            self.tela
            ,text=">>"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: self._logica(1)
        )
        self.Badicionar = tk.Button(
            self.tela
            ,text="Adicionar"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            ,padx= 0
            ,pady= 0

            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,command=lambda: self.textSemana.config(state= "normal")
        )
        self.Btexcluir = tk.Button(
            self.tela
            ,text="Excluir"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            # ,command=lambda: self.start_UI()
        )
        self.BTSalvar = tk.Button(
            self.tela
            ,text="Salvar"
            ,cursor="hand2"
            ,relief="solid"
            ,font=("Arial", 10, "bold")
            ,highlightthickness=3
            
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            # ,command=lambda: self.start_UI()
        )

        pass
    def Localizar(self):
        self.tela.place(relx= 0.01, rely= 0.01, relheight= 0.98, relwidth= 0.98)
        
        self.logSemana.place(relx= 0.06, rely= 0.01, relheight= 0.15, relwidth= 0.16)
        self.textSemana.place(relx= 0.28, rely= 0.01, relheight= 0.98, relwidth= 0.71)

        self.BTanterior.place(relx= 0.01, rely= 0.01, relheight= 0.15, relwidth= 0.05)
        self.BtProximo.place(relx= 0.22, rely= 0.01, relheight= 0.15, relwidth= 0.05)
        self.Badicionar.place(relx= 0.01, rely= 0.20, relheight= 0.15, relwidth= 0.26)
        self.Btexcluir.place(relx= 0.01, rely= 0.40, relheight= 0.15, relwidth= 0.26)
        self.BTSalvar.place(relx= 0.01, rely= 0.60, relheight= 0.15, relwidth= 0.26)
    pass
class Modulos(_Auxiliares):
    def __init__(self, telas):
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 16, "bold"), "justify": "center"}

        self.valida_cmd = telas.register(_Auxiliares._verificar_limite)
        self.fonte = font.Font(family="verdana", size= 10)

        self.TelaModulos(telas)
        self.Clicaveis()
        self.localizador()
        pass
    
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
            ,variable= tk.BooleanVar(value=False)
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
            ,command=lambda: Jornadas(self.AlaAlterar)
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
        self.AlaBotao.place(relx= 0.01, rely= 0.01, relheight= 0.10, relwidth= 0.98)
        self.AlaAlterar.place(relx= 0.01, rely= 0.12, relheight= 0.85, relwidth= 0.98)

        # Loclização dos botão

        self.BTModulos.place(relx= 0.05, rely= 0.05, relheight= 0.90, relwidth= 0.20)
        self.BTJornadas.place(relx= 0.30, rely= 0.05, relheight= 0.90, relwidth= 0.20)
        self.BTRotinas.place(relx= 0.55, rely= 0.05, relheight= 0.90, relwidth= 0.20)
        pass


if __name__ == "__main__":
    PainelConfigurar()