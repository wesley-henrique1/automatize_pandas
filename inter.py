import tkinter as tk


class App:
    def __init__(self):
        self.background = "#2F4F4F"
        self.frame_color = "#F0FFFF"
        self.borda_color = "#000000"
        self.back_2 = "#363636"
        self.estilo_alerta = {"foreground": "#FF640A", "font": ("Consolas", 12, "bold")}

        root = tk.Tk()
        root.title("Provisorio")
        root.geometry("396x400")
        root.config(bg= self.background)
        root.resizable(True, True)
        

        

        btn = tk.Button(root, text="Ver Tamanho", command=lambda: self.mostrar_tamanho(root))
        btn.pack(pady=20)
        root.mainloop()
        pass

    def mostrar_tamanho(self, janela):
        janela.update() 
        largura = janela.winfo_width()
        altura = janela.winfo_height()
        print(f"Largura: {largura}px, Altura: {altura}px")

    def Componentes(self, janela):
        self.quadro = tk.Frame(
            janela
            ,bg= self.back_2
            ,highlightbackground= self.borda_color
            ,highlightthickness= 3
        )
        self.entry_cod = tk.Text(
            self.frame_fundo
            ,font=("Consolas", 10)
            ,bg=self.frame_color
            ,fg=self.borda_color
            ,highlightbackground=self.borda_color
            ,highlightthickness=2
            ,padx=10, pady=10
        )
        pass
    def Clicaveis(self):
        pass
    def Localizar(self):
        pass


if __name__ == "__main__":
    App()