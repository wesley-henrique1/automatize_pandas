from MODULOS.config_path import *
import datetime as dt
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

class auxiliar:
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
        if id == 1:
            temp = df.groupby(col).agg(
                QTDE_OS = ("NUMOS", 'nunique'),
                OS_50 = ("Tipo O.S.", lambda x: (x == '50 - Movimentação De Para').sum()),
                OS_61 = ("Tipo O.S.", lambda x: (x == '61 - Movimentação De Para Horizontal').sum()),
                OS_58 = ("Tipo O.S.", lambda x: (x == '58 - Transferencia de Para Vertical').sum())
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

class BI_ABST(auxiliar):
    def __init__(self):        

        self.pipeline()
        
    
    def pipeline(self):
        try:
            func = pd.read_excel(outros.ou_func, sheet_name='FUNC')
            cons28 = pd.read_excel(power_bi.abst_cons28)
            m_atual28 = pd.read_excel(power_bi.abst_atual28)
            cons64 = pd.read_excel(power_bi.abst_cons64)
            m_atual64 = pd.read_excel(power_bi.abst_atual64)
            configurar_mes = 12
        except Exception as e:
            self.validar_erro(e, "EXTRAIR")
            return False

        try:
            try:
                m_atual28 = self.organizar_df(m_atual28,'DATA',1)
                cons28 = self.organizar_df(cons28,'DATA',1)
                cons28 = cons28.loc[cons28['MES'] != configurar_mes]
                concat28 = pd.concat([cons28, m_atual28], ignore_index= True)

                os_geradas28 = self.agrupar(concat28, ['DATA','CODFUNCGER'], 1)

                os_finalizadas28 = self.agrupar(concat28, ['DATA','CODFUNCOS'], 1)

                pendencia = concat28.loc[concat28['POSICAO'] =='P']
                os_pedentes28 = self.agrupar(pendencia, ['DATA','CODFUNCGER'], 1)        
            except Exception as e:
                self.validar_erro(e, "TRATAMENTO_28")
                return False


            try:
                m_atual64['CODEMPILHADOR'] = m_atual64['CODEMPILHADOR'].fillna(0)  
                m_atual64 = self.organizar_df(m_atual64, 'DATAGERACAO', 2)
                cons64 = self.organizar_df(cons64, 'DATAGERACAO', 2)

                cons64 = cons64.loc[cons64['MES'] != configurar_mes]
                concat64 = pd.concat([cons64, m_atual64], ignore_index= True)
                concat64 = concat64.rename(columns={'DATAGERACAO' : "DATA"})

                agrupamento = self.agrupar(concat64, ['DATA', 'CODEMPILHADOR'], 2).fillna(0)
                agrupamento['CODFUNCGER'] = 1

                os_finalizadas64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] != 0]
                os_pedentes64 = agrupamento.loc[agrupamento['CODEMPILHADOR'] == 0]
            except Exception as e:
                self.validar_erro(e, "TRATAMENTO_64")
                return False
            try:
                func = func.loc[func['AREA'] == 'EXPEDICAO']

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

                bonus = self.agrupar(concat64, 'DTLANC', 5)
            except Exception as e:
                self.validar_erro(e, "TRATAMENTO_GERAL")
                return False
        except Exception as e:
            self.validar_erro(e, "TRATAMENTO")
            return False
        try:
            
            pd_total.to_excel(power_bi.abst_pd, index= False, sheet_name= "OS_PD")
            fim_total.to_excel(power_bi.abst_fim, index= False, sheet_name= "OS_FIM")
            geral_total.to_excel(power_bi.abst_geral, index= False, sheet_name= "OS_GERAL")
            bonus.to_excel(power_bi.abst_bonus, index= False, sheet_name= "BONUS")       
        except Exception as e:
            self.validar_erro(e, "CARGA")
            return False

        try:
            data_max28 = concat28['DATA'].max()
            data_min28 = concat28['DATA'].min()

            data_max64 = concat64['DATA'].max()
            data_min64 = concat64['DATA'].min()
            print("_" * 36)
            print("\nOperação finalizada...")
            print("Periodo calculdado de cada base:")
            print(f"8628: {data_min28:%d-%m-%Y} - {data_max28:%d-%m-%Y}")
            print(f"8664: {data_min64:%d-%m-%Y} - {data_max64:%d-%m-%Y}")
        except Exception as e:
            self.erro = Funcao.validar_erro(e)
            print(f"Apresentação: {self.erro}")

if __name__ == "__main__":
    BI_ABST()
    input("\nPressione Enter para finalizar...")