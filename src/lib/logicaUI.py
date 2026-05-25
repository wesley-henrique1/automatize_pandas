from tkinter import messagebox
import threading
from .valerros import ValidarErros

class Processador:
    validador = ValidarErros(fonte="main_logica")

    def __init__(self, UI):
        self._widget_ancora = UI

    def _executar_na_main_thread(self, func, *args, **kwargs):
        self._widget_ancora.retorno.after(0, lambda: func(*args, **kwargs))
    def _alterar_estado_botoes(self, estado):
        self._widget_ancora.bt_iniciar.config(state=estado)
        self._widget_ancora.bt_limpar.config(state=estado)
        pass
    def _atualizar_contador(self, texto):
        self._widget_ancora.contador.config(text=texto)
        pass
    def _inserir_texto_no_widget(self, widget, conteudo, limpar=True):
        widget.config(state="normal")
        if limpar:
            widget.delete("1.0", "end")
        widget.insert("end", conteudo)
        widget.config(state="disabled")
        widget.see("end")
        pass
    def _gerar_cabecalho_tabela(self, colunas, comprimentos):
        linhas = [f"{col:^{w}}" for col, w in zip(colunas, comprimentos)]
        separador = "-" * (sum(comprimentos) + (len(comprimentos) - 1) * 3)
    
        return f"{' | '.join(linhas)}\n{separador}\n" 
    def _processar_finalizacao(self, lista_de_logs, lista_de_file, dic_log, log_data, msg_corte):
        """Centraliza o encerramento do processo e distribui as atualizações para as respectivas telas."""
        try:       
            logs_unicos = list({log['ARQUIVO']: log for log in lista_de_logs if isinstance(log, dict)}.values())
            logs_file = list({log['MODULO']: log for log in lista_de_file if isinstance(log, dict)}.values())
            
            resumo = "\n".join([f"{modulo}: {status}" for modulo, status in dic_log.items()])

            def acoes_finais_ui():
                self._atualizar_logs(logs_unicos)
                self._atualizar_db(logs_file)
                self._atualizar_output(log_data)
                
                messagebox.showinfo("Resumo da Operação", resumo)
                
                if msg_corte is not None:
                    self._widget_ancora.tela_CORTE("Relatório de Corte", msg_corte)
                
                self._alterar_estado_botoes("normal")

            self._executar_na_main_thread(acoes_finais_ui)

        except Exception as e:
            self.validador.registrar_log(e, "TASK_RETORNO")


    def _atualizar_logs(self, dados_arquivos):
        """Tela 1: Formata e renderiza exclusivamente a tabela de arquivos em self._widget_ancora.retorno"""
        try:
            if not dados_arquivos:
                self._widget_ancora._exibir_mensagem_status(" >>> NENHUM ARQUIVO PROCESSADO")
                return
            
            conteudo = self._gerar_cabecalho_tabela(["ID", "ARQUIVO", "DATA", "HORA"], [3, 40, 10, 8])

            for item in dados_arquivos:
                nome_arq = str(item.get('ARQUIVO', 'DESCONHECIDO'))
                nome_arq = nome_arq[:37] + "..." if len(nome_arq) > 37 else nome_arq
                
                id_log = item.get('CONTADOR', 0)
                date = item.get('DATA', '--/--/----')
                hrs = item.get('HORAS', '--:--')
                
                conteudo += f"{id_log:03d} | {nome_arq:<40} | {date:<10} | {hrs:<8}\n"

            self._inserir_texto_no_widget(self._widget_ancora.retorno, conteudo, limpar=True)

        except Exception as e:
            self.validador.registrar_log(e, "Atualizar LOG Arquivos")
            self._widget_ancora._exibir_mensagem_status(" >>> ERRO AO GERAR LOG DE ARQUIVOS.")
        pass
    def _atualizar_db(self, dados_modulos):
        if not dados_modulos:
            return      
        try:
            conteudo_log = self._gerar_cabecalho_tabela(["MODULO", "ARQUIVOS", "ERROS", "LEITURAS"], [6, 7, 5, 8])
            
            for file in dados_modulos:
                modulo = file.get("MODULO", "DESCONHECIDO")
                contador = file.get("ARQUIVOS", 0)
                fora = file.get("ERROS", 0)
                qtde = file.get("LEITURA", 0)
                
                conteudo_log += f"{modulo:^6} | {contador:^8} | {fora:^5} | {qtde:^8}\n"

            self._inserir_texto_no_widget(self._widget_ancora.retorno_db, conteudo_log, limpar=True)
        except Exception as e:
            self.validador.registrar_log(e, "Atualizar LOG Banco de Dados")
        pass
    def _atualizar_output(self, data_periodo):
        if not data_periodo:
            return
        try:
            conteudo_dt = f"{'-' * 75}\n{data_periodo}"
            self._inserir_texto_no_widget(self._widget_ancora.retorno, conteudo_dt, limpar=False)
        except Exception as e:
            self.validador.registrar_log(e, "Atualizar Output Dados")

    def task_workflow(self, argumento):
        lista_de_logs = []
        lista_de_file = []
        dic_log = {}
        msg_corte = None
        log_data = None
        
        total_scripts = len(argumento)

        if total_scripts == 0:
            self._executar_na_main_thread(self._atualizar_contador, "PROGRESSO >> 0% || 0/0 OPERAÇÃO")
            self._executar_na_main_thread(self._alterar_estado_botoes, "normal")
            return

        for i, nome in enumerate(argumento):
            try:
                progresso_anterior = (i / total_scripts) * 100
                txt_progresso = f"PROGRESSO >> {progresso_anterior:.0f}% -> {nome} || {i}/{total_scripts} OPERAÇÃO"
                self._executar_na_main_thread(self._atualizar_contador, txt_progresso)

                classe_do_script = self._widget_ancora.scripts_map[nome]
                instancia = classe_do_script()

                if nome == "abstKeys":
                    status_pipeline, log_data = instancia.pipeline() 
                else:
                    status_pipeline = instancia.pipeline()
                
                log_arquivo, log_db = instancia.carregamento()
                
                if nome == "CorteKeys" and status_pipeline is not False:
                    msg_corte = instancia.Log_Retorno()

                dic_log[nome] = "Executado" if status_pipeline else "Travado (Erro Interno)"

                if log_arquivo:
                    lista_de_logs.extend(log_arquivo if isinstance(log_arquivo, list) else [log_arquivo])
                if log_db:
                    lista_de_file.extend(log_db if isinstance(log_db, list) else [log_db])

                progresso_atual = ((i + 1) / total_scripts) * 100
                txt_final = f"PROGRESSO >> {progresso_atual:.0f}% -> {nome} || {i + 1}/{total_scripts} OPERAÇÃO"
                self._executar_na_main_thread(self._atualizar_contador, txt_final)

            except Exception as e:
                print("erro")
                dic_log[nome] = "Falha Crítica"
                self.validador.registrar_log(e, f"Módulo: {nome}")
                self._executar_na_main_thread(self._widget_ancora._exibir_mensagem_status," >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt"
                )

        self._processar_finalizacao(lista_de_logs, lista_de_file, dic_log, log_data, msg_corte)

    def executar_threads(self, selecionados):
        self._alterar_estado_botoes("disabled")
        thread = threading.Thread(
            target=self.task_workflow, 
            args=(selecionados,), 
            daemon=True
        )
        thread.start()

