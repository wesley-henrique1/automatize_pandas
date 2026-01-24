from modulos._settings import Power_BI,Outros, Relatorios
import datetime as dt
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

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
    def organizar_df(self, df_original, col, id):
        df = df_original
        df[col] = pd.to_datetime(df[col]).dt.normalize()
        df['MES'] = df[col].dt.month

        if id == 1:
            df = df[['NUMOS', 'DATA', 'CODROTINA', 'POSICAO', 'CODFUNCGER', 'FUNCGER','DTFIMOS', 'CODFUNCOS', 'FUNCOSFIM', 'Tipo O.S.', 'TIPOABAST', 'MES']].copy()
            df = df.loc[(df['CODROTINA'] == 1709) | (df['CODROTINA'] == 1723)]
            return df
        elif id ==2: 
            df = df[['DATAGERACAO', 'DTLANC', 'NUMBONUS', 'NUMOS', 'CODEMPILHADOR','EMPILHADOR', 'MES']].copy()
            df = df.drop_duplicates(subset=['NUMOS'])
            return df
    def agrupar(self, df, col, id):
        df = df.copy()
        if id == 1:
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
        elif id == 2:
            temp = df.groupby(col).agg(
                OS_RECEB = ("NUMOS", 'nunique')
            ).reset_index()
            return temp
        elif id == 3:
            temp = df.groupby(col).agg(
                OS_RECEB = ('OS_RECEB', 'sum')
            ).reset_index()
            temp[col] = pd.to_datetime(temp[col]).dt.normalize()
            return temp
        elif id == 4:
            temp = df.groupby(col).agg(
                QTDE_OS = ("QTDE_OS", 'sum'),
                OS_50 = ("OS_50", 'sum'),
                OS_61 = ("OS_61", 'sum'),
                OS_58 = ("OS_58", 'sum')
            ).reset_index()
            temp[col] = pd.to_datetime(temp[col]).dt.normalize()
            return temp 
        elif id == 5:
            temp = df.groupby([col]).agg(
                    TOTAL_BONUS = ("NUMBONUS", "nunique")
                ).reset_index().sort_values(by=col, ascending= False)  
            return temp 
class Abastecimento(auxiliar):
    def __init__(self):
        self.list_path = [Outros.ou_func, Power_BI.abst_atual28, Power_BI.abst_atual64]
        self.pipeline()

    def pipeline(self):
        try:
            cols_28 = ['NUMOS', 'DATA', 'CODROTINA', 'POSICAO', 'CODFUNCGER', 'FUNCGER', 
            'DTFIMOS', 'CODFUNCOS', 'FUNCOSFIM', 'Tipo O.S.', 'TIPOABAST']
            cols_64 = ['DATAGERACAO', 'DTLANC', 'NUMBONUS', 'NUMOS', 'CODEMPILHADOR', 'EMPILHADOR']

            # Execução da leitura
            m_atual28 = pd.read_excel(Relatorios.rel_28, usecols=cols_28, engine='openpyxl')
            m_atual64 = pd.read_excel(Relatorios.rel_64, usecols=cols_64, engine='openpyxl')

            dt_maior_28 = m_atual28['DATA'].max()
            dt_menor_28 = m_atual28['DATA'].min()

            dt_maior_64 = m_atual64['DATAGERACAO'].max()
            dt_menor_64 = m_atual64['DATAGERACAO'].min()
            periodo = (
                f"\n8628 >> {dt_menor_28:%d/%m/%Y} -- {dt_maior_28:%d/%m/%Y}\n"
                f"8664 >> {dt_menor_64:%d/%m/%Y} -- {dt_maior_64:%d/%m/%Y}"
            )
        except Exception as e:
            self.validar_erro(e, "Extract")
            return False

        try:
            try:
                m_atual28 = self.organizar_df(m_atual28,'DATA',1)
                os_geradas28 = self.agrupar(m_atual28, ['DATA','CODFUNCGER'], 1)
                os_finalizadas28 = self.agrupar(m_atual28, ['DATA','CODFUNCOS'], 1)

                pendencia = m_atual28.loc[m_atual28['POSICAO'] =='P']
                os_pedentes28 = self.agrupar(pendencia, ['DATA','CODFUNCGER'], 1)
            except Exception as e:
                self.validar_erro(e, "T_28")
                return False
            try:
                m_atual64['CODEMPILHADOR'] = m_atual64['CODEMPILHADOR'].fillna(0)  
                m_atual64 = self.organizar_df(m_atual64, 'DATAGERACAO', 2)
                m_atual64 = m_atual64.rename(columns={'DATAGERACAO' : "DATA"})
                
                agrupamento = self.agrupar(m_atual64, ['DATA', 'CODEMPILHADOR'], 2).fillna(0)
                agrupamento['CODFUNCGER'] = 1

                os_finalizadas64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] != 0]
                os_pedentes64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] == 0]
            except Exception as e:
                self.validar_erro(e, "T_64")
                return False
            try:
                """Ordem de serviço pendentes"""
                temp28 = os_pedentes28[['DATA','CODFUNCGER','QTDE_OS']]
                temp64 = os_pedentes64[['DATA','CODFUNCGER','OS_RECEB']]
                temp64 = temp64.rename(columns={ 'OS_RECEB': 'QTDE_OS'})
                pd_total = pd.concat([temp28, temp64], ignore_index= True)

                """Ordem de serviço finalizadas"""
                os_finalizadas64 = os_finalizadas64.rename(columns={'CODEMPILHADOR': 'CODFUNCOS'})
                fim_total = os_finalizadas28.merge(os_finalizadas64, left_on=['DATA','CODFUNCOS'], right_on= ['DATA','CODFUNCOS'], how= 'left').drop(columns='CODFUNCGER').fillna(0)

                """Ordem de serviço geradas"""
                grupo64 = self.agrupar(agrupamento, 'DATA', 3)
                grupo28 = self.agrupar(os_geradas28, 'DATA', 4)

                geral_total = grupo28.merge(grupo64, left_on='DATA', right_on= 'DATA', how= 'left').fillna(0)

                bonus = self.agrupar(m_atual64, 'DTLANC', 5)
            except Exception as e:
                self.validar_erro(e, "T_GERAL")
                return False
        except Exception as e:
            self.validar_erro(e, "Transform")
            return False
        try:
            pd_total.to_excel(Power_BI.abst_pd, index= False, sheet_name= "OS_PD")
            fim_total.to_excel(Power_BI.abst_fim, index= False, sheet_name= "OS_FIM")
            geral_total.to_excel(Power_BI.abst_geral, index= False, sheet_name= "OS_GERAL")
            bonus.to_excel(Power_BI.abst_bonus, index= False, sheet_name= "BONUS")
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
