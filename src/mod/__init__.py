import importlib
import json

from ..lib.settings import Assets
from ..lib import ValidarErros


def Executar():
    validador = ValidarErros(fonte="mod")
    base = Assets.JsonModulo

    with open(base, 'r', encoding= 'utf-8') as var:
        Modular = json.load(var)
    listKeys = []
    dicModular = {}
    dicName = {}

    for var in Modular.keys():
        listKeys.append(var)

    for var in listKeys:
        try:
            if Modular[var]['_estado'] == 'true':
                ChaveModulo = Modular[var]['_chave']
                NomeModulo = Modular[var]['_Modulo']
                NomeClasse = Modular[var]['_classes']
                NomeClatura = Modular[var]['_Nomeclatura']

                dicName[ChaveModulo] = NomeClatura      

                modulo_carregado = importlib.import_module(f'.{NomeModulo}', package="src.mod")  
                ClasseFisica = getattr(modulo_carregado, NomeClasse)        
                dicModular[ChaveModulo] = ClasseFisica
        except Exception as e:
            validador.registrar_log(e, "_init_")
    return dicModular, dicName

