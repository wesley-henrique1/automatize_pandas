import pyautogui as pag
import pyperclip as pc
import threading
import time
from ..lib import ValidarErros
from tkinter import messagebox
import tkinter as tk

"""
    objetivo: automação para excluir endereços na rotina 3706 em massa

    interface: a UI vai ter um componente para capturar os codigos, duas fleg para informar se vai ser master ou venda, botão de iniciar e parar, registro de progresso,

"""


class Flow3706:
    validador = ValidarErros(fonte="Flow 3706")
    def __init__(self):
        pass

    def PararProcesso(self):
        pass
    def IniciarProcesso(self, _listas):
        pass

    def _automact(self,lista_sku, maximo):
        pass