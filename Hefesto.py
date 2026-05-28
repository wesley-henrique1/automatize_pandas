class forja:
    def __init__(self, Interface):
        self.modular = Interface

        # Vincula os widgets de texto da sua Main UI
        self.labelLog = self.modular.retorno
        self.labelDB = self.modular.retorno_db
        self.labelTemp = self.modular.retorno_file

    def _ajustar(self):
        self.labelLog.config(state="normal")
        self.labelDB.config(state="normal")
        self.labelTemp.config(state="normal")

        self.labelLog.delete("1.0", "end")
        self.labelDB.delete("1.0", "end")
        self.labelTemp.delete("1.0", "end")

        msg_temp = (
            f"{'ARQUIVO':^21} | {'DATA':^10} | {'HORA':^8}\n"
            f"{'-' * 45}\n"
            f"{"BI_ABASTECIMENTO.xlsx":<21} | {"22/05/2026":^10} | {"07:37:59":^8}\n"
        )
        
        msg_db= (
            f"{'MODULO':^15} | {'ARQUIVOS':^8} | {'ERROS':^5} | {'LEITURAS':^9}\n"
            f"{"-" * 45}\n"
            f"{'Corte':<15} | {'0':^8} | {'0':^5} | {'101':^9}\n"
        )
        
        msg_log = (
            f"{'ARQUIVO':^48} | {'DATA':^12} | {'HORA':^8}\n"
            f"{'-' * 76}\n"
            f"{"1707 - Relatório de produtos por endereços.txt":<48} | {"22/05/2026":^12} | {"07:37:59":^8}\n"
        )


