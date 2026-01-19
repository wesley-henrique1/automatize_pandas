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
        msg_corte = None
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

                log_arquivo = instancia.carregamento()
                status_pipeline = instancia.pipeline()

                if nome == "Corte" and status_pipeline is not False:
                    msg_corte = instancia.Log_Retorno()
                    dic_log[nome] = "Executado"
                elif status_pipeline:
                    dic_log[nome] = "Executado"
                elif not status_pipeline:
                    dic_log[nome] = "Travado (Erro Interno)"

                if isinstance(log_arquivo, list):
                    lista_de_logs.extend(log_arquivo)
                elif isinstance(log_arquivo, dict):
                    lista_de_logs.append(log_arquivo)

                progresso_atual = ((i + 1) / total_scripts) * 100
                txt_final = f"{progresso_atual:.0f}% -> {nome}"
                self.mainUI.retorno.after(0, lambda p=txt_final: self.mainUI.contador.config(text=p))
            except Exception as e:
                dic_log[nome] = "Falha Crítica"
                self.validar_erro(e, f"Módulo: {nome}")
                self.mainUI._exibir_mensagem_status(" >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt")
                
        logs_unicos = {log['ARQUIVO']: log for log in lista_de_logs}.values()
        self.mainUI.retorno.after(0, lambda: self.atualizar_log(logs_unicos))

        resumo = "\n".join([f"{modulo}: {status}" for modulo, status in dic_log.items()])
        self.mainUI.retorno.after(100, lambda: messagebox.showinfo("Resumo da Operação", resumo))

        if msg_corte is not None:
            self.mainUI.retorno.after(200, lambda: self.mainUI.segunda_tela("Relatório de Corte", msg_corte))

        self.mainUI.retorno.after(300, finalizar)
    def atualizar_log(self, dados_arquivos):
        try:
            dados_validos = [d for d in dados_arquivos if isinstance(d, dict)]
            if not dados_validos:
                self._exibir_mensagem_status(" >>> NENHUM DADO PROCESSADO")
                return

            conteudo = f"{'ID':^3} | {'ARQUIVO':^46} | {'DATA':^10} | {'HORA':^8}\n"
            conteudo += f"{'-' * 76}\n"

            for item in dados_validos:
                nome_arq = str(item.get('ARQUIVO', 'DESCONHECIDO'))
                if len(nome_arq) > 41:
                    nome_arq = nome_arq[:43] + "..."
                linha = (
                    f"{item.get('CONTADOR', 0):02d}  | {nome_arq:<46} | "
                    f"{item.get('DATA', '--/--/----'):<10} | {item.get('HORAS', '--:--'):<8}\n")
                conteudo += linha
            self.mainUI.retorno.config(state="normal")
            self.mainUI.retorno.delete("1.0", "end")
            self.mainUI.retorno.insert("end", conteudo)
            self.mainUI.retorno.config(state="disabled")
            self.mainUI.retorno.see("end")
        except Exception as e:
            self.validar_erro(e, "Atualizar LOG")
            self._exibir_mensagem_status(" >>> ERRO AO GERAR LOG. VERIFIQUE log_erros.txt")
    
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
    
    
    
