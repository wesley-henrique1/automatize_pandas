# from modulos._settings import Power_BI, Relatorios
from _settings import Power_BI, Relatorios
import datetime as dt
import pandas as pd
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
"""
    Refazer o script separando os funcionarios por turno e depois realizando a media de saida

"""
class auxiliar:
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
        agora = dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
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
    def agrupar(self, df, col):
        df = df.copy()

        df['IS_50'] = (df['Tipo O.S.'] == '50 - Movimentação De Para').astype(int)
        df['IS_61'] = (df['Tipo O.S.'] == '61 - Movimentação De Para Horizontal').astype(int)
        df['IS_58'] = (df['Tipo O.S.'] == '58 - Transferencia de Para Vertical').astype(int)
        temp = df.groupby(col).agg(
            QTDE_OS = ("NUMOS", 'nunique'),
            OS_50 = ("IS_50", "sum"),
            OS_61 = ("IS_61", "sum"),
            OS_58 = ("IS_58", "sum")
        ).reset_index()
        return temp
    def corrigir_hr(self, valor):
        valor = str(valor).strip()
        
        separador = valor.split(":")
        
        if len(separador) == 2:
            # Caso: "7:15" -> vira "07:15:00"
            horas = separador[0].zfill(2)
            minuto = separador[1].zfill(2)
            segundo = "00"
            return f"{horas}:{minuto}:{segundo}"
        
        elif len(separador) == 3:
            # Caso: "7:15:5" -> vira "07:15:05"
            horas = separador[0].zfill(2)
            minuto = separador[1].zfill(2)
            segundo = separador[2].zfill(2)
            return f"{horas}:{minuto}:{segundo}"
    
        return valor
class Abastecimento(auxiliar):
    def __init__(self):
        self.list_path = [Relatorios.rel_28, Relatorios.rel_64]
        self.rotinas_filtro = [1723,1709]
        self.hr_AM = "06:30:00"
        self.hr_PM = "18:00:00"
        self.pipeline()

    def pipeline(self):
        try:
            cols_28 = ['NUMOS', 'DATA','HORA', 'CODROTINA', 'POSICAO', 'CODFUNCGER', 'FUNCGER', 
            'DTFIMOS', 'CODFUNCOS', 'FUNCOSFIM', 'Tipo O.S.', 'TIPOABAST']
            cols_64 = ['DATAGERACAO', 'DTLANC', 'NUMBONUS', 'NUMOS', 'CODEMPILHADOR', 'EMPILHADOR']

            m_atual28 = pd.read_excel(self.list_path[0], usecols=cols_28, engine='openpyxl')
            m_atual64 = pd.read_excel(self.list_path[1], usecols=cols_64, engine='openpyxl')

            dt_maior_28 = m_atual28['DATA'].max()
            dt_menor_28 = m_atual28['DATA'].min()

            dt_maior_64 = m_atual64['DATAGERACAO'].max()
            dt_menor_64 = m_atual64['DATAGERACAO'].min()
            periodo = (
                f"\n8628 >> {dt_menor_28:%d/%m/%Y} -- {dt_maior_28:%d/%m/%Y}\n"
                f"8664 >> {dt_menor_64:%d/%m/%Y} -- {dt_maior_64:%d/%m/%Y}"
            )
            print("PASSAGEM EXTRAÇÃO\n")
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False

        try:
            try:
                m_atual28['DATA'] = pd.to_datetime(m_atual28['DATA']).dt.normalize()
                m_atual28['MES'] = m_atual28['DATA'].dt.month
                m_atual28['HORA'] = m_atual28["HORA"].apply(self.corrigir_hr)
                m_atual28 = m_atual28.loc[m_atual28['CODROTINA'].isin(self.rotinas_filtro)]
                
                cond_turno = (
                    (m_atual28['HORA'] >= self.hr_AM)  & (m_atual28['HORA'] <= self.hr_PM)  # CONDIÇÃO 1
                    ,(m_atual28['HORA'] >= self.hr_PM) | (m_atual28['HORA'] < self.hr_AM)   # CONDIÇÃO 2
                )
                result_turno = (
                    m_atual28['DATA']                               # CONDIÇÃO 1
                    ,m_atual28['DATA'] - pd.Timedelta(days= 1)      # CONDIÇÃO 2
                )
                m_atual28['DT_TURNO'] = np.select(cond_turno, result_turno, default=m_atual28['DATA'])
                pendencia = m_atual28.loc[m_atual28['POSICAO'] == "P"]


                os_geradas28 = self.agrupar(m_atual28, ['DT_TURNO','CODFUNCGER'])
                os_finalizadas28 = self.agrupar(m_atual28, ['DT_TURNO','CODFUNCOS'])
                os_pedentes28 = self.agrupar(pendencia, ['DT_TURNO','CODFUNCGER'])
                print(f"debug: T_28")
            except Exception as e:
                self.validar_erro(e, "T_28")
                print(f"debug T_28: {e}")
                return False
            try:
                m_atual64['CODEMPILHADOR'] = m_atual64['CODEMPILHADOR'].fillna(0)  
                m_atual64['DATAGERACAO'] = pd.to_datetime(m_atual64['DATAGERACAO']).dt.normalize()
                m_atual64['MES'] = m_atual64['DATAGERACAO'].dt.month
                m_atual64 = m_atual64[['DATAGERACAO', 'DTLANC', 'NUMBONUS', 'NUMOS', 'CODEMPILHADOR','EMPILHADOR', 'MES']].copy()
                m_atual64 = m_atual64.drop_duplicates(subset=['NUMOS'])

                m_atual64['DATA'] = pd.to_datetime(m_atual64['DATAGERACAO'].dt.date)
                m_atual64['HORA'] = m_atual64['DATAGERACAO'].dt.time
                m_atual64['HORA'] = m_atual64["HORA"].apply(self.corrigir_hr)
                cond_turno = (
                    (m_atual64['HORA'] >= self.hr_AM)  & (m_atual64['HORA'] <= self.hr_PM)
                    ,(m_atual64['HORA'] >= self.hr_PM) | (m_atual64['HORA'] < self.hr_AM)
                )
                result_turno = (
                    m_atual64['DATA']
                    ,m_atual64['DATA'] - pd.Timedelta(days= 1)
                )
                m_atual64['DT_TURNO'] = np.select(cond_turno, result_turno, default=m_atual64['DATA'])
                
                agrupamento = m_atual64.groupby(['DT_TURNO', 'CODEMPILHADOR']).agg(
                    OS_RECEB = ("NUMOS", 'nunique')
                ).reset_index().fillna(0)
                agrupamento['CODFUNCGER'] = 1

                os_finalizadas64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] != 0]
                os_pedentes64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] == 0]
            except Exception as e:
                self.validar_erro(e, "T_64")
                return False
            try:
                """Ordem de serviço pendentes"""
                temp28 = os_pedentes28[['DT_TURNO','CODFUNCGER','QTDE_OS']]
                temp64 = os_pedentes64[['DT_TURNO','CODFUNCGER','OS_RECEB']]
                temp64 = temp64.rename(columns={ 'OS_RECEB': 'QTDE_OS'})
                pd_total = pd.concat([temp28, temp64], ignore_index= True)

                """Ordem de serviço finalizadas"""
                os_finalizadas64 = os_finalizadas64.rename(columns={'CODEMPILHADOR': 'CODFUNCOS'})
                fim_total = os_finalizadas28.merge(os_finalizadas64, left_on=['DT_TURNO','CODFUNCOS'], right_on= ['DT_TURNO','CODFUNCOS'], how= 'left').drop(columns='CODFUNCGER').fillna(0)

                """Ordem de serviço geradas"""
                grupo64 = agrupamento.groupby('DT_TURNO').agg(
                    OS_RECEB = ('OS_RECEB', 'sum')
                ).reset_index()
                grupo64['DT_TURNO'] = pd.to_datetime(grupo64['DT_TURNO']).dt.normalize()

                grupo28 = os_geradas28.groupby('DT_TURNO').agg(
                    QTDE_OS = ("QTDE_OS", 'sum'),
                    OS_50 = ("OS_50", 'sum'),
                    OS_61 = ("OS_61", 'sum'),
                    OS_58 = ("OS_58", 'sum')
                ).reset_index()
                grupo28['DT_TURNO'] = pd.to_datetime(grupo28['DT_TURNO']).dt.normalize()
                
                geral_total = grupo28.merge(grupo64, left_on='DT_TURNO', right_on= 'DT_TURNO', how= 'left').fillna(0)
            except Exception as e:
                print(f"debug T_GERAL: {e}")
                self.validar_erro(e, "T_GERAL")
                return False
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            pd_total.to_excel(Power_BI.abst_pd, index= False, sheet_name= "OS_PD")
            fim_total.to_excel(Power_BI.abst_fim, index= False, sheet_name= "OS_FIM")
            geral_total.to_excel(Power_BI.abst_geral, index= False, sheet_name= "OS_GERAL")
            print("Laod - finish")
            return True, periodo
        except Exception as e:
            self.validar_erro(e, "Laod")
            return False
    def carregamento(self):
        lista_de_logs = []
        dic_retorno = []
        return lista_de_logs, dic_retorno

if __name__ == "__main__":
    Abastecimento()
