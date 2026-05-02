from modulos._settings import Assets
from datetime import datetime as dt
import json
import time

class MonitorETL:
    def __init__(self):
        self.log  = Assets.LogTime
        self.__notbook = {
            "Extract": [],
            "Transform": [],
            "Load": []
        }
        pass
    def validar_erro(self, e, etapa):
        largura = 64
        mapeamento = {
            PermissionError: "Arquivo aberto ou sem permissão. Feche o Excel.",
            FileNotFoundError: "Arquivo de origem não encontrado. Verifique a pasta 'base_dados'.",
            KeyError: f"Coluna ou chave não encontrada: {e}",
            TypeError: f"Incompatibilidade de tipo: {e}",
            ValueError: f"Formato de dado inválido: {e}",
            NameError: f"Variável ou função não definida: {e}"
        }
        
        msg = mapeamento.get(type(e), f"Erro não mapeado: {e}")
        agora = dt.now().strftime('%d/%m/%Y %H:%M:%S')
        
        log_conteudo = (
            f"{'='* largura}\n"
            f"FONTE: abastecimento.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")
    
    def __FormatTime(self, time_seconds):
        hr = int(time_seconds // 3600)
        min = int((time_seconds % 3600) // 60)
        seg = int(time_seconds % 60)

        msg = f"{hr:02d}:{min:02d}:{seg:02d}"
        return msg
    
        pass
    def __NovoRegistro(self, Data, Hora, Modulos, Extract, Transform, Load, Total):
        try:
            with open(self.log , 'r', encoding= 'utf-8') as ws:
                df_JSON = json.load(ws)

            df_JSON['Data'].append(Data)
            df_JSON['Hora'].append(Hora)
            df_JSON['Modulos'].append(Modulos)
            df_JSON['Extract'].append(Extract)
            df_JSON['Transform'].append(Transform)
            df_JSON['Load'].append(Load)
            df_JSON['Total'].append(Total)

            with open(self.log , 'w', encoding='utf-8') as ws:
                json.dump(df_JSON, ws, indent=4)
        except Exception as e:
            self.validar_erro(e, "TimeJson")
        
        pass
    
    def stageTime(self, etapa: str):
        self.__notbook[etapa].append(time.perf_counter())
        print(self.__notbook)

        pass
    def conversor(self, Modulo: str):
        dic = self.__notbook
        agora = dt.now()
        TData = agora.strftime('%d/%m/%Y')
        Thora = agora.strftime('%H:%M:%S')

        TExtract = self.__FormatTime(dic['Extract'][1] - dic['Extract'][0])
        TTransform = self.__FormatTime(dic['Transform'][1] - dic['Transform'][0])
        TLoad = self.__FormatTime(dic['Load'][1] - dic['Load'][0])
        TTotal = self.__FormatTime(dic['Load'][1] - dic['Extract'][0])
        for conteudo in dic.values():
            conteudo.clear()

        self.__NovoRegistro(TData, Thora, Modulo, TExtract, TTransform, TLoad, TTotal)