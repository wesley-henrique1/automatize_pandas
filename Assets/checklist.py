from modulos._settings import Path_dados
import tkinter as tk
from datetime import datetime as dt, timedelta

class Demandas:
    def __init__(self):
        segunda = [
            "RELATORIO: CORTE"
            ,"RELATORIO: ABASTECIMENTO"
            ,"RELATORIO: GIRO ESTOQUE"
            ,"FEFO: CURVA VENCIMENTO"
            ,"CONTAGEM ETIQUETAS"
            ,"INVENTARIO: FL || ATIVO"
            ,"CADASTRO: CAPACIDADE"
        ]
        terca = [
            "RELATORIO: CORTE"
            ,"RELATORIO: ABASTECIMENTO"
            ,"FEFO: RETIRADA CURVA A"
            ,"RELATORIO O.S MENSAL"
            ,"RELATORIO: O.S PEDENTES || FIM MESA"
            ,"RELATORIO: CHEIO X VAZIO"
            ,"CADASTRO: VALIDAR FLEGS"
            ,"INVENTARIO: BAIXO ESTOQUE"
        ]
        quarta = [
            "RELATORIO: CORTE"
            ,"RELATORIO: ABASTECIMENTO"
            ,"RELATORIO: O.S PEDENTES || FIM MESA"
            ,"RELATORIO: CHEIO X VAZIO"
            ,"FEFO: ANÁLISE 'CURVA DE SHELF'"
            ,"CADASTRO: CUBAGEM"
        ]
        quinta = [
            "RELATORIO: CORTE"
            ,"RELATORIO: ABASTECIMENTO"
            ,"RELATORIO: O.S PEDENTES || FIM MESA"
            ,"RELATORIO: CHEIO X VAZIO"
            ,"FEFO: ANÁLISE 'CURVA DE SHELF' E DIVERGÊNCIAS (SISTEMA X FISICO)"
            ,"CADASTRO: CORREÇÃO"
            ,"INVENTARIO: ESTOQUE PARADO"
        ]
        sexta = [
            "RELATORIO: CORTE"
            ,"RELATORIO: ABASTECIMENTO"
            ,"RELATORIO: O.S PEDENTES || FIM MESA"
            ,"RELATORIO: CHEIO X VAZIO"
            ,"FEFO: AEREO MENOR QUE PICKING"
            ,"CADASTRO: CORREÇÃO"
            ,"INVENTARIO: ACURACIDADE"
        ]
        self.DEMANDAS_SEMANAIS = {
            2: segunda
            ,3: terca
            ,4: quarta
            ,5: quinta
            ,6: sexta
        }
        self.dias_nome = {
            2: "Segunda-feira"
            , 3: "Terça-feira"
            , 4: "Quarta-feira"
            , 5: "Quinta-feira"
            , 6: "Sexta-feira"
        }
        
        self.fonte = ("verdana", 9,"bold") 
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}
        hoje = dt.now()
        dia_hoje = hoje.weekday() + 2
        self.demandas = self.DEMANDAS_SEMANAIS.get(dia_hoje, ["Sem tarefas hoje"])
        self.nome_dia = self.dias_nome.get(dia_hoje, "Final de Semana")

        root = tk.Toplevel()
        root.title("CHECKLIST TAREFAS")
        root.geometry("400x440")
        root.resizable(False, False)
        root.config(bg= self.background)
        root.iconbitmap(Path_dados.icone_pricipal)

        self.componentes(root)
        self.localizador()
        root.mainloop()
        

    def componentes(self, tela):
        self.chek_frame = tk.Frame(
            tela
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.lbl_titulo = tk.Label(
            self.chek_frame
            ,text= f"Demandas: {self.nome_dia}"
            ,font= self.fonte
            ,bg= self.back_2
            ,fg= self.frame_color
        )        
        self.list_place = []
        for tarefa in self.demandas:            
            check = tk.Checkbutton(
                self.chek_frame, 
                text= tarefa, 
                font= self.fonte,
                anchor= "w",
                bg= self.frame_color ,
            )
            self.list_place.append(check)

    def localizador(self):
        self.chek_frame.place(rely= 0.02, relx= 0.02, relwidth= 0.96, relheight= 0.96)

        self.lbl_titulo.place(rely= 0.02, relx= 0.02, relwidth= 0.96, relheight= 0.10)

        eixo_y = 0.15
        salto = 0.08    
        eixo_x = 0.02
        for cap in self.list_place:
            cap.place(rely= eixo_y, relx= eixo_x)
            eixo_y += salto
if __name__ == "__main__":
    Demandas()
