import json
import datetime as dt
from .settings import Assets

class ValidarErros:
    def __init__(self, e=None, etapas=None, fonte="ModuloExterno"):
        with open(Assets.JsonErros, 'r', encoding='utf-8') as var:
            self.mapeamento = json.load(var)

        self.fonte = fonte

        if e is not None and etapas is not None:
            self.registrar_log(e, etapas)

    def registrar_log(self, e, etapa):
        largura = 77
        nome_do_erro = type(e).__name__
        msg = self.mapeamento.get(nome_do_erro, "Ocorreu um erro inesperado no sistema.")
        agora = dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        log_conteudo = (
            f"{'='* largura}\n"
            f"FONTE: {self.fonte} | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {nome_do_erro}\n"
            f"MENSAGEM: {msg}\n"
            f"DETALHE TÉCNICO: {e}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
            print(f" Log gravado para a etapa: {etapa}")
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")