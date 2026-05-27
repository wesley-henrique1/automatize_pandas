
from ..lib.settings import Relatorios, OutPut
from ..lib import ValidarErros, MonitorETL

import pandas as pd
import numpy as np

class auxiliar:
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
        
        if len(separador) == 1:
            return f"{separador[0].zfill(2)}:00:00"
        
        elif len(separador) == 2:
            return f"{separador[0].zfill(2)}:{separador[1].zfill(2)}:00"
        
        elif len(separador) == 3:
            return f"{separador[0].zfill(2)}:{separador[1].zfill(2)}:{separador[2].zfill(2)}"

        return valor
class Abastecimento(auxiliar):
    validador = ValidarErros(fonte="Abastecimento")
    def __init__(self):
        self.list_path = [Relatorios._8628, Relatorios._8664]
        self.rotinas_filtro = [1723,1709]
        self.hr_AM = "06:30:00"
        self.hr_PM = "18:00:00"

        self.Instancia = MonitorETL()

    def pipeline(self):
        try:
            self.Instancia.stageTime('Extract')
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
            self.Instancia.stageTime('Extract')
        except Exception as e:
            self.validador.registrar_log(e, "Extract")
            return False

        try:
            self.Instancia.stageTime('Transform')
            try:
                m_atual28['DATA'] = pd.to_datetime(m_atual28['DATA'], dayfirst= True).dt.normalize()
                m_atual28['MES'] = m_atual28['DATA'].dt.month
                m_atual28['HORA'] = m_atual28["HORA"].apply(self.corrigir_hr)
                m_atual28 = m_atual28.loc[m_atual28['CODROTINA'].isin(self.rotinas_filtro)]

                cond_turno = [
                    (m_atual28['HORA'] >= "00:00:00")  & (m_atual28['HORA'] < self.hr_AM)  # CONDIÇÃO 1
                ]
                result_turno = [
                    m_atual28['DATA'] - pd.Timedelta(days=1)
                ]
                m_atual28['DT_TURNO'] = np.select(cond_turno, result_turno, default=m_atual28['DATA'])
                pendencia = m_atual28.loc[m_atual28['POSICAO'] == "P"]

                os_geradas28 = self.agrupar(m_atual28, ['DT_TURNO','CODFUNCGER'])
                os_finalizadas28 = self.agrupar(m_atual28, ['DT_TURNO','CODFUNCOS'])
                os_pedentes28 = self.agrupar(pendencia, ['DT_TURNO','CODFUNCGER'])
            except Exception as e:
                self.validador.registrar_log(e, "T_28")
                return False
            try:
                m_atual64['CODEMPILHADOR'] = m_atual64['CODEMPILHADOR'].fillna(0)  
                m_atual64['DATAGERACAO'] = pd.to_datetime(m_atual64['DATAGERACAO'], dayfirst= True)
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
                self.validador.registrar_log(e, "T_64")
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
                grupo64['DT_TURNO'] = pd.to_datetime(grupo64['DT_TURNO'], dayfirst= True).dt.normalize()

                grupo28 = os_geradas28.groupby('DT_TURNO').agg(
                    QTDE_OS = ("QTDE_OS", 'sum'),
                    OS_50 = ("OS_50", 'sum'),
                    OS_61 = ("OS_61", 'sum'),
                    OS_58 = ("OS_58", 'sum')
                ).reset_index()
                grupo28['DT_TURNO'] = pd.to_datetime(grupo28['DT_TURNO'], dayfirst= True).dt.normalize()
                
                geral_total = grupo28.merge(grupo64, left_on='DT_TURNO', right_on= 'DT_TURNO', how= 'left').fillna(0)
            except Exception as e:
                self.validador.registrar_log(e, "T_GERAL")
                return False
            self.Instancia.stageTime('Transform')
        except Exception as e:
            self.validador.registrar_log(e, "Transform")
            return False
        try:
            self.Instancia.stageTime('Load')
            with pd.ExcelWriter(OutPut.Abastecimento) as var:
                pd_total.to_excel(var, index= False, sheet_name= "OS_PD")
                fim_total.to_excel(var, index= False, sheet_name= "OS_FIM")
                geral_total.to_excel(var, index= False, sheet_name= "OS_GERAL")
            self.Instancia.stageTime('Load')
            self.Instancia.conversor(Modulo= "Abastecimento")
            return True, periodo
        except Exception as e:
            self.validador.registrar_log(e, "Load")
            return False
    def carregamento(self):
        lista_de_logs = []
        dic_retorno = []
        return lista_de_logs, dic_retorno