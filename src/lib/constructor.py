"""
    Recriação do modulo logicaUI afim de melhorar a leitura e excluir redundancias
"""
from ..lib import ValidarErros

class ForjaUI:
    validador = ValidarErros(fonte="main")
    def __init__(self, Conectar):
        self.ListaLog = []
        self.ListaDb = []
        self.ListaOutPut = []
        self.ancora = Conectar
        print("Aqui foi iniciado o processo")
        self.MapeamentoModular = self.ancora.scripts_map

        self.ClickIniciar = self.ancora.bt_iniciar
        self.ClickLimpar = self.ancora.bt_limpar

        self.labelLog = self.ancora.retorno
        self.labelDB = self.ancora.retorno_db
        self.labelTemp = self.ancora.retorno_file
        pass

    def _AlimentarLog(self, _log):
        """
        Função para alimentar UI com arquivos usado
        dicionario = {
            "ARQUIVO": None,
            "DATA": None,
            "HORA": None
        }
        """

        titulo = f"{'ARQUIVO':^48} | {'DATA':^12} | {'HORA':^8}\n"
        linha = f"{'-' * 76}\n"
        tabela = titulo + linha

        for var in _log:
            _arquivo = var.get('ARQUIVO', "Arquivo não encontrado")
            _data = var.get('DATA', '--/--/----')
            _hora = var.get('HORA', '--:--:--')

            conteudo = f"{_arquivo:<48} | {_data:^12} | {_hora:^8}\n"

            tabela += conteudo

        self.labelLog.config(state="normal")
        self.labelLog.delete("1.0", "end")
        self.labelLog.insert("end", tabela)
        self.labelLog.config(state="disabled")
        pass
    def _AlimentarDB(self, _log):
        """
        Função para alimentar UI com retorno database
        dicionarios = {
        "MODULO": None,
        "ARQUIVOS": None,
        "ERROS": None,
        "LEITURAS": None
        }
        """

        titulo = f"{'MODULO':^15} | {'ARQUIVOS':^8} | {'ERROS':^5} | {'LEITURAS':^9}\n"
        linha = f"{"-" * 45}\n"
        tabela = titulo + linha

        for var in _log:
            _modulo = var.get('MODULO', "Modulo não encontrado")
            _arquivos = var.get('ARQUIVOS', '-')
            _erros = var.get('ERROS', '-')
            _leituras = var.get('LEITURAS', "-")

            conteudo = f"{_modulo:<15} | {_arquivos:^8} | {_erros:^5} | {_leituras:^9}\n"

            tabela += conteudo

        self.labelDB.config(state="normal")
        self.labelDB.delete("1.0", "end")

        self.labelDB.insert("end", tabela)
        self.labelDB.config(state="disabled")

        pass
    def _AlimentarOutPut(self, _log):
        """Função para alimentar UI com retorno do pipeline"""
        """
        dicionario = {
            "ARQUIVO": None,
            "DATA": None,
            "HORA": None
        }
        """

        titulo = f"{'ARQUIVO':^21} | {'DATA':^10} | {'HORA':^8}\n"
        linha = f"{'-' * 45}\n"
        tabela = titulo + linha

        for var in _log:
            _arquivo = var.get('ARQUIVO', "Arquivo não encontrado")
            _data = var.get('DATA', '--/--/----')
            _hora = var.get('HORA', '--:--:--')

            conteudo = f"{_arquivo:<21} | {_data:^10} | {_hora:^8}\n"

            tabela += conteudo
        
        self.labelTemp.config(state="normal")
        self.labelTemp.delete("1.0", "end")
        
        self.labelTemp.insert("end", tabela)
        self.labelTemp.config(state="disabled")
        pass
    
    def IniciarProcesso(self):
        msgDB = [
            {
                "MODULO": 'Corte',
                "ARQUIVOS": '0',
                "ERROS": '0',
                "LEITURAS": '101'
            },
            {
                "MODULO": 'None',
                "ARQUIVOS": '0',
                "ERROS": '101',
                "LEITURAS": '0'
            }
        ]
        msgTemp = [
            {
            "ARQUIVO": "BI_ABASTECIMENTO.xlsx",
            "DATA": "22/05/2026",
            "HORA": "07:37:59"
            },
            {
            "ARQUIVO": "BI_algumacoisa.xlsx",
            "DATA": "22/05/2026",
            "HORA": "07:37:59"
            }
        ] 
        msg_log = [
            {
                "ARQUIVO": "1707 - Relatório de produtos por endereços.txt",
                "DATA": "22/05/2026",
                "HORA": "07:37:59"
            },
            {
                "ARQUIVO": "1708 - Relatório de produtos por endereços.xlsx",
                "DATA": "22/05/2026",
                "HORA": "07:37:59"
            }
        ]
        try:
            print("Aqui foi passado para função")

            self._AlimentarDB(msgDB)
            self._AlimentarOutPut(msgTemp)
            self._AlimentarLog(msg_log)
        except Exception as e:
            self.validador.registrar_log(e, f"inico eeeeeee")

        pass


