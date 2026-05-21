from .settings import Assets
from .valerros import ValidarErros

from datetime import datetime as dt
import json
import time

class MonitorETL:
    validador = ValidarErros(fonte="MonitorETL")
    def __init__(self):
        self.log  = Assets.JsonLogTime
        self.__notbook = {
            "Extract": [],
            "Transform": [],
            "Load": []
        }
        pass
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
            self.validador.registrar_log(e, "TimeJson")
        
        pass
    
    def stageTime(self, etapa: str):
        self.__notbook[etapa].append(time.perf_counter())

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