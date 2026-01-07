from MODULOS.config_path import Output, Relatorios, Wms, ColNames, Outros
import datetime as dt
import pandas as pd

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

def app():

    try:
        df_life = pd.read_excel(Outros.ou_life, usecols=['CONFERENCIA','PRODUTO','VALIDADE','PROBLEMA'])
        df_bonus = pd.read_excel(Relatorios.rel_64)
        df_end = pd.read_csv(Wms.wms_07_end, header= None, names= ColNames.col_end)
        df_str = pd.read_excel(Outros.ou_end, sheet_name= "AE", usecols= ['COD_END', 'RUA'])
    except Exception as e:
        error = validar_erro(e)
        print(F"EXTRAÇÃO: {error}")

    try:
        df_life['COD'] = (df_life['PRODUTO'].str.split('-').str[0]).astype(int)
        df_life['VALIDADE'] = pd.to_datetime(df_life['VALIDADE'], errors='coerce')
        df_life["ID_LIFE"] = df_life['CONFERENCIA'].astype(str) + " - " + df_life['COD'].astype(str) + " - " + df_life['VALIDADE'].astype(str)

        df_prod = df_bonus[['NUMBONUS', 'CODPROD', 'DESCRICAO', 'CODENDERECO', 'DTVALIDADE']].copy()
        df_prod['DTVALIDADE'] = pd.to_datetime(df_prod['DTVALIDADE'], errors='coerce')
        df_prod["ID_LIFE"] = df_prod['NUMBONUS'].astype(str) + " - " + df_prod['CODPROD'].astype(str) + " - " + df_prod['DTVALIDADE'].astype(str)

        merge = df_life.merge(df_prod, left_on='ID_LIFE', right_on='ID_LIFE', how= 'inner')
        merge = merge[['CODPROD', 'DESCRICAO', 'CODENDERECO','NUMBONUS', 'DTVALIDADE','PROBLEMA']]
        merge['ID_END'] = merge['CODENDERECO'].astype(str) + " - " + merge['CODPROD'].astype(str)

        aereo = df_end.merge(df_str, left_on='COD_END', right_on='COD_END', how='left')
        aereo['ID_END'] = aereo['COD_END'].astype(str) + " - " + aereo['COD'].astype(str)
        aereo = aereo[['COD','COD_END','RUA','PREDIO', 'NIVEL', 'APTO', 'DT.VALIDADE','ID_END']]

        df_fim = merge.merge(aereo, left_on='ID_END', right_on='ID_END', how= 'inner')
        df_fim = df_fim[['CODPROD', 'DESCRICAO', 'NUMBONUS', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'DTVALIDADE','DT.VALIDADE','PROBLEMA']]


        df_filtrado = df_fim.drop_duplicates(subset=['CODPROD', 'DTVALIDADE'], keep='first')
    except Exception as e:
        error = validar_erro(e)
        print(F"TRATAMENTO: {error}")

    try:
        df_filtrado.to_excel(Output.life, sheet_name="DIVERGENCIA", index=False)
    except Exception as e:
        error = validar_erro(e)
        print(F"CARGA: {error}")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")