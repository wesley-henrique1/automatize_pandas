import tkinter as tk

background = "#0C316B"

bg_label = "#0C316B"
fg_label = "#FFFFFF"

bg_text = "#FFFFFF"
fg_text = "#000000"

class fleg_app():
    def __init__(self, root):
        self.root = root
        self.root.title("Controle de Inventário")
        self.root.configure(background= background)
        self.root.geometry("330x300")
        self.root.resizable(False, False)

        self.interface()

    def interface(self):
        self.observacao = tk.Frame(self.root, text="Aguardando códigos...", bg= bg_label, fg= fg_label)
        self.observacao.place(relx=0.050, rely=0.005, relwidth=0.900, relheight=0.200)

        self.text_codprod = tk.Text(self.root, height=10, width=40,bd= 2, bg= bg_text, fg= fg_text)
        self.text_codprod.place(relx= 0.050, rely= 0.250, relwidth=0.900, relheight=0.500)

        self.contador = tk.Label(self.root, text="Contador: 0/0", bg= bg_label, fg= fg_label)
        self.contador.place(relx=0.050, rely=0.770, relwidth=0.900, relheight=0.050)

        self.btn_iniciar = tk.Button(self.root, text="Iniciar Transferência", bg= bg_text, fg= fg_text)
        self.btn_iniciar.place(relx=0.050, rely=0.870, relwidth=0.900, relheight=0.100)

class t3707():
    def __init__(self, root):
        self.root = root
        self.root.title("Transferência de Códigos")
        self.root.configure(background="#0C316B")
        self.root.geometry("330x500")
        self.root.resizable(False, False)

        self.interface()

    def interface(self):
        self.frame = tk.Frame(self.root, bg= bg_label)
        self.frame.place(relx=0.005, rely=0.005, relwidth=0.99, relheight=0.99)

        self.text_codprod = tk.Text(self.frame, bg= bg_text, fg= fg_label, bd=2, font=("Verdana", 10))
        self.text_codprod.place(relx=0.010, rely=0.100, relwidth=0.490, relheight=0.700)

        self.text_codend = tk.Text(self.frame, bg= bg_text, fg= fg_label, bd=2, font=("Verdana", 10))
        self.text_codend.place(relx=0.500, rely=0.100, relwidth=0.490, relheight=0.700)

        self.botao = tk.Button(self.root, text="Transferir", bg= bg_text, fg= fg_text, 
                             bd=3, font=("Verdana", 10))
        self.botao.place(relx=0.050, rely=0.030, relwidth=0.250, relheight=0.050)

        self.contador = tk.Label(self.root, text="Contador: 0/0", bg= bg_label, fg= fg_label)
        self.contador.place(relx=0.650, rely=0.030, relwidth=0.260, relheight=0.050)

        self.observacao = tk.Label(self.root, text="ATENÇÃO: \n", bg= bg_label, fg= fg_label, font=("Verdana", 8), bd=4, anchor="nw")
        self.observacao.place(relx=0.020, rely=0.810, relwidth=0.960, relheight=0.150)

if __name__ == "__main__":
    root = tk.Tk()
    app = t3707(root)
    root.mainloop()