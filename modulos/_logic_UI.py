from tkinter import messagebox
import datetime as dt
import threading

class ProcessadorLogica:
    def __init__(self, UI):
        self.mainUI = UI
        self._widget_ancora = self.mainUI.retorno

        pass

    def _executar_na_main_thread(self, func, *args, **kwargs):
        self._widget_ancora.after(0, lambda: func(*args, **kwargs))
        
        pass
    def _alterar_estado_botoes(self, estado):
        self.mainUI.bt_iniciar.config(state=estado)
        self.mainUI.bt_limpar.config(state=estado)
        
        pass
    def _atualizar_contador(self, texto):
        self.mainUI.contador.config(text=texto)
        pass
    
    
    def executar_threads(self, selecionados):
        self._alterar_estado_botoes("disabled")
        
        thread = threading.Thread(
            target=self.task_workflow, 
            args=(selecionados,), 
            daemon=True
        )
        thread.start()
        
        pass
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
                print("teste de passagem")

                progresso_anterior = (i / total_scripts) * 100
                txt_progresso = f"PROGRESSO >> {progresso_anterior:.0f}% -> {nome} || {i}/{total_scripts} OPERAÇÃO"
                self._executar_na_main_thread(self._atualizar_contador, txt_progresso)

                classe_do_script = self.mainUI.scripts_map[nome]
                instancia = classe_do_script()

                print("trageto instancia")

                if nome == "Abastecimento":
                    status_pipeline, log_data = instancia.pipeline() 
                else:
                    status_pipeline = instancia.pipeline()

                    print("trageto pipeline")

                log_arquivo, log_db = instancia.carregamento()
                
                print("trageto carregamento")

                if nome == "Corte" and status_pipeline is not False:
                    msg_corte = instancia.Log_Retorno()
                    dic_log[nome] = "Executado"
                else:
                    dic_log[nome] = "Executado" if status_pipeline else "Travado (Erro Interno)"

                if log_arquivo:
                    lista_de_logs.extend(log_arquivo if isinstance(log_arquivo, list) else [log_arquivo])
                if log_db:
                    lista_de_file.extend(log_db if isinstance(log_db, list) else [log_db])

                progresso_atual = ((i + 1) / total_scripts) * 100
                txt_final = f"PROGRESSO >> {progresso_atual:.0f}% -> {nome} || {i + 1}/{total_scripts} OPERAÇÃO"
                self._executar_na_main_thread(self._atualizar_contador, txt_final)

            except Exception as e:
                dic_log[nome] = "Falha Crítica"
                self.validar_erro(e, f"Módulo: {nome}")
                self._executar_na_main_thread(self.mainUI._exibir_mensagem_status, " >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt")

        self._processar_finalizacao(lista_de_logs, lista_de_file, dic_log, log_data, msg_corte)
        
        pass
    def _processar_finalizacao(self, lista_de_logs, lista_de_file, dic_log, log_data, msg_corte):
        try:       
            logs_unicos = list({log['ARQUIVO']: log for log in lista_de_logs}.values())
            logs_file = list({log['MODULO']: log for log in lista_de_file}.values())
            resumo = "\n".join([f"{modulo}: {status}" for modulo, status in dic_log.items()])

            self._executar_na_main_thread(self.atualizar_log, logs_unicos, log_data)
            self._executar_na_main_thread(self.Atualizar_db, logs_file)
            
            self._widget_ancora.after(100, lambda: messagebox.showinfo("Resumo da Operação", resumo))
            
            if msg_corte is not None:
                self._widget_ancora.after(200, lambda: self.mainUI.tela_CORTE("Relatório de Corte", msg_corte))


            self._widget_ancora.after(300, lambda: self._alterar_estado_botoes("normal"))
        except Exception as e:
            self.validar_erro(e, "TASK_RETORNO")
        
        pass
    def atualizar_log(self, dados_arquivos, data_periodo):
        try:
            dados_validos = [d for d in dados_arquivos if isinstance(d, dict)]

            if not dados_validos and not data_periodo:
                self.mainUI._exibir_mensagem_status(" >>> NENHUM ARQUIVO PROCESSADO")
                return
            
            if dados_validos:
                conteudo = f"{'ID':^3} | {'ARQUIVO':^45} | {'DATA':^10} | {'HORA':^8}\n"
                conteudo += f"{'-' * 75}\n"

                for item in dados_validos:
                    nome_arq = str(item.get('ARQUIVO', 'DESCONHECIDO'))
                    nome_arq = nome_arq[:42] + "..." if len(nome_arq) > 41 else nome_arq
                    
                    id_log = item.get('CONTADOR', 0)
                    date = item.get('DATA', '--/--/----')
                    hrs = item.get('HORAS', '--:--')
                    
                    conteudo += f"{id_log:03d} | {nome_arq:<45} | {date:<10} | {hrs:<8}\n"

                self._inserir_texto_no_widget(self.mainUI.retorno, conteudo, limpar=True)

            if data_periodo:
                conteudo_dt = f"{'-' * 75}\n{data_periodo}"
                self._inserir_texto_no_widget(self.mainUI.retorno, conteudo_dt, limpar=False)

        except Exception as e:
            self.validar_erro(e, "Atualizar LOG")
            self.mainUI._exibir_mensagem_status(" >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt")
        
        pass
    def Atualizar_db(self, dados_modulos):
        ar_validos = [modulo for modulo in dados_modulos if isinstance(modulo, dict)]
        if not ar_validos:
            return      
        try:
            conteudo_log = f"{'MODULO':^6} | {'ARQUIVOS':^7} | {'ERROS':5} | {'LEITURAS':^8}\n"
            conteudo_log += f"{'-' * 36}\n"
            
            for file in ar_validos:
                modulo = file.get("MODULO", "DESCONHECIDO")
                contador = file.get("ARQUIVOS", 0)
                fora = file.get("ERROS", 0)
                qtde = file.get("LEITURA", 0)
                
                conteudo_log += f"{modulo:^6} | {contador:^8} | {fora:^5} | {qtde:^8}\n"

            self._inserir_texto_no_widget(self.mainUI.retorno_db, conteudo_log, limpar=True)
        except Exception as e:
            self.validar_erro(e, "LOG-ERRO")
        
        pass
    def _inserir_texto_no_widget(self, widget, conteudo, limpar=True):
        widget.config(state="normal")
        if limpar:
            widget.delete("1.0", "end")
        widget.insert("end", conteudo)
        widget.config(state="disabled")
        widget.see("end")
        
        pass
    def validar_erro(self, e, etapa):
        largura = 78
        mapeamento = {
            PermissionError: "Arquivo aberto ou sem permissão. Feche o Excel.",
            FileNotFoundError: "Arquivo de origem não encontrado. Verifique a pasta 'base_dados'.",
            KeyError: f"Coluna ou chave não encontrada: {e}",
            TypeError: f"Incompatibilidade de tipo: {e}",
            ValueError: f"Formato de dado inválido: {e}",
            NameError: f"Variável ou função não definida: {e}"
        }
        
        msg = mapeamento.get(type(e), f"Erro não mapeado: {e}")
        agora = dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        log_conteudo = (
            f"{'='* largura}\n"
            f"FONTE: _logic_UI.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )
        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")