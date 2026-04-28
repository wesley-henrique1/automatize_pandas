from modulos._settings import Path_dados
from datetime import datetime as dt, timedelta
import tkinter as tk
import json

class Auxiliar:
    def _json(self):
        try:
            with open(Path_dados.J_jornada, 'r', encoding= 'utf-8') as jornada:
                dados_brutos = json.load(jornada)
                dic_json = {int(k): v for k, v in dados_brutos.items()}
        except:
            print("erro", "\narquivo não encotrado")
            dic_json = {}
        return dic_json
    def atualizar_interface(self):
        dia_hoje = self.hoje.weekday() + 2
        self.demandas = self.DEMANDAS_SEMANAIS.get(dia_hoje, ["Sem tarefas hoje"])
        self.nome_dia = self.dias_nome.get(dia_hoje, "Final de Semana")
        self.lbl_titulo.config(text=f"Demandas: {self.nome_dia}")
        for check in self.list_place:
            check.destroy()
        self.list_place = []
        for tarefa in self.demandas:            
            check = tk.Checkbutton(
                self.chek_frame, 
                text=tarefa, 
                font=self.fonte,
                anchor="w",
                bg=self.frame_color,
            )
            self.list_place.append(check)
        self.renderizar_checks()

    def renderizar_checks(self):
        eixo_y = 0.15
        salto = 0.08    
        eixo_x = 0.02
        for cap in self.list_place:
            cap.place(rely=eixo_y, relx=eixo_x)
            eixo_y += salto
    def Proximo(self):
        self.hoje += timedelta(days=1)
        self.atualizar_interface()
    def Anterior(self):
        self.hoje -= timedelta(days=1)
        self.atualizar_interface()    
        
    pass
class Demandas(Auxiliar):
    def __init__(self):
        self.DEMANDAS_SEMANAIS = self._json()
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

        self.hoje = dt.now()

        dia_hoje = self.hoje.weekday() + 2
        self.demandas = self.DEMANDAS_SEMANAIS.get(dia_hoje, ["Sem tarefas hoje"])
        self.nome_dia = self.dias_nome.get(dia_hoje, "Final de Semana")

        root = tk.Tk()
        root.title("CHECKLIST TAREFAS")
        root.geometry("400x440")
        root.resizable(False, False)
        root.config(bg= self.background)
        root.iconbitmap(Path_dados.icone_pricipal)

        self.componentes(root)
        self.Clicaveis()
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
    def Clicaveis(self):
        self.btProximo = tk.Button(
            self.chek_frame
            ,text= ">"
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
            ,command= self.Proximo
        )
        self.btAnterior = tk.Button(
            self.chek_frame
            ,text= "<"
            ,bg= self.frame_color
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
            ,command= self.Anterior
        )

    def localizador(self):
        self.chek_frame.place(rely= 0.02, relx= 0.02, relwidth= 0.96, relheight= 0.96)

        self.lbl_titulo.place(rely= 0.02, relx= 0.14, relwidth= 0.72, relheight= 0.10)
        self.btAnterior.place(rely= 0.02, relx= 0.02, relwidth= 0.10, relheight= 0.10)
        self.btProximo.place(rely= 0.02, relx= 0.88, relwidth= 0.10, relheight= 0.10)

        eixo_y = 0.15
        salto = 0.08    
        eixo_x = 0.02
        for cap in self.list_place:
            cap.place(rely= eixo_y, relx= eixo_x)
            eixo_y += salto
if __name__ == "__main__":
    Demandas()
