from tkinter import messagebox
import datetime as dt
import threading

class ProcessadorLogica:
    def __init__(self, UI):
        self.mainUI = UI

    def executar_threads(self, selecionados):
        self.mainUI.bt_iniciar.config(state="disabled")
        self.mainUI.bt_limpar.config(state="disabled")
        
        thread = threading.Thread(
            target=self.task_workflow, 
            args=(selecionados,), 
            daemon=True
        )
        thread.start()
    def task_workflow(self, argumento):
        lista_de_logs = []
        lista_de_file = []

        msg_corte = None
        log_data = None
        dic_log = {}
        total_scripts = len(argumento)
        def finalizar():
            self.mainUI.bt_iniciar.config(state="normal")
            self.mainUI.bt_limpar.config(state="normal")

        if total_scripts == 0:
            self.mainUI.retorno.after(0, lambda: self.mainUI.contador.config(text="0%"))
            self.mainUI.retorno.after(0, finalizar)
            return

        for i, nome in enumerate(argumento):
            try:
                progresso_anterior = (i / total_scripts) * 100
                self.mainUI.retorno.after(
                    0
                    , lambda p=progresso_anterior
                    , n=nome: self.mainUI.contador.config(text=f"{p:.0f}% -> {n}")
                )
                classe_do_script = self.mainUI.scripts_map[nome]
                instancia = classe_do_script()
                
                if nome == "Abastecimento":
                    status_pipeline, log_data = instancia.pipeline()  
                else:
                    status_pipeline = instancia.pipeline()  


                log_arquivo, log_db = instancia.carregamento()


                if nome == "Corte" and status_pipeline is not False:
                    msg_corte = instancia.Log_Retorno()
                    dic_log[nome] = "Executado"
                elif status_pipeline:
                    dic_log[nome] = "Executado"
                elif not status_pipeline:
                    dic_log[nome] = "Travado (Erro Interno)"

                if log_arquivo:
                    if isinstance(log_arquivo, list):
                        lista_de_logs.extend(log_arquivo)
                    else:
                        lista_de_logs.append(log_arquivo)
                if log_db:
                    if isinstance(log_db, list):
                        lista_de_file.extend(log_db)
                    else:
                        lista_de_file.append(log_db)

                progresso_atual = ((i + 1) / total_scripts) * 100
                txt_final = f"{progresso_atual:.0f}% -> {nome}"
                self.mainUI.retorno.after(0, lambda p=txt_final: self.mainUI.contador.config(text=p))
            except Exception as e:
                dic_log[nome] = "Falha Crítica"
                self.validar_erro(e, f"Módulo: {nome}")
                self.mainUI._exibir_mensagem_status(" >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt")
        try:       
            logs_unicos = {log['ARQUIVO']: log for log in lista_de_logs}.values()
            logs_file = {log['MODULO']: log for log in lista_de_file}.values()
            resumo = "\n".join([f"{modulo}: {status}" for modulo, status in dic_log.items()])

            self.mainUI.retorno.after(0, lambda: self.atualizar_log(dados_arquivos=logs_unicos, dados_modulos= logs_file, data_periodo= log_data))
            self.mainUI.retorno.after(100, lambda: messagebox.showinfo("Resumo da Operação", resumo))
            if msg_corte is not None:
                self.mainUI.retorno.after(200, lambda: self.mainUI.segunda_tela("Relatório de Corte", msg_corte))

            self.mainUI.retorno.after(300, finalizar)
        except Exception as e:
            self.validar_erro(e, "TASK_RETORNO")
    def atualizar_log(self, dados_arquivos, dados_modulos, data_periodo):
        try:
            dados_validos = [dados for dados in dados_arquivos if isinstance(dados, dict)]
            ar_validos = [modulo for modulo in dados_modulos if isinstance(modulo, dict)]

            if not dados_validos and not ar_validos:
                self.mainUI._exibir_mensagem_status(" >>> NENHUM ARQUIVO PROCESSADO")
                return
            
            if dados_validos:
                try:
                    conteudo = f"{'ID':^3} | {'ARQUIVO':^46} | {'DATA':^10} | {'HORA':^8}\n"
                    conteudo += f"{'-' * 76}\n"
                    print(type(conteudo))

                    for item in dados_validos:
                        nome_arq = str(item.get('ARQUIVO', 'DESCONHECIDO'))
                        if len(nome_arq) > 41:
                            nome_arq = nome_arq[:43] + "..."
                        id = item.get('CONTADOR', 0)
                        date = item.get('DATA', '--/--/----')
                        hrs = item.get('HORAS', '--:--')
                        linha = (
                            f"{id:03d} | {nome_arq:<46} | {date:<10} | {hrs:<8}\n")
                        conteudo += linha 

                    
                    print(conteudo)
                    self.mainUI.retorno.config(state="normal")
                    self.mainUI.retorno.delete("1.0", "end")
                    self.mainUI.retorno.insert("end", conteudo)
                    self.mainUI.retorno.config(state="disabled")
                    self.mainUI.retorno.see("end")
                except Exception as e:
                    self.validar_erro(e, "LOG-RETORNO")
            if data_periodo:
                try:
                    conteudo_dt = f"{'-' * 76}\n"
                    conteudo_dt += data_periodo

                    self.mainUI.retorno.config(state="normal")
                    self.mainUI.retorno.insert("end", conteudo_dt)
                    self.mainUI.retorno.config(state="disabled")
                    self.mainUI.retorno.see("end")
                except Exception as e:
                    self.validar_erro(e, "LOG-RETORNO")

            if ar_validos:
                try:
                    conteudo_log = f"{"MODULO":^6} | {"ARQUIVOS":^8} | {"ERROS":5} | {"LEITURAS":^8}\n"
                    divisa = f"{"-" * 36}\n"
                    conteudo_log += divisa
                    for file in ar_validos:
                        modulo = file.get("MODULO", "DESCONHECIDO")
                        contador = file.get("ARQUIVOS", 0)
                        fora = file.get("ERROS", 0)
                        qtde = file.get("LEITURA", 0)
                        linha_log = f"{modulo:^6} | {contador:^8} | {fora:^5} | {qtde:^8}\n"
                        conteudo_log += linha_log

                    self.mainUI.retorno_db.config(state="normal")
                    self.mainUI.retorno_db.delete("1.0", "end")
                    self.mainUI.retorno_db.insert("end", conteudo_log)
                    self.mainUI.retorno_db.config(state="disabled")
                    self.mainUI.retorno_db.see("end")
                except Exception as e:
                    self.validar_erro(e, "LOG-ERRO")
        except Exception as e:
            self.validar_erro(e, "Atualizar LOG")
            self.mainUI._exibir_mensagem_status(" >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt")
    
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
            f"FONTE: main.py | ETAPA: {etapa} | DATA: {agora}\n"
            f"TIPO: {type(e).__name__}\n"
            f"MENSAGEM: {msg}\n"
            f"{'='* largura}\n\n"
        )

        try:
            with open("log_erros.txt", "a", encoding="utf-8") as f:
                f.write(log_conteudo)
        except Exception as erro_f:
            print(f"Falha crítica ao gravar log: {erro_f}")
    
    
    
