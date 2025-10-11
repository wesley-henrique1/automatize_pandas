from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np
import warnings
import glob
import os 
warnings.simplefilter(action='ignore', category=UserWarning)

def validar_erro(e):
    print("=" * 60)
    if isinstance(e, KeyError):
        return f"KeyError: A coluna ou chave '{e}' não foi encontrada."
    elif isinstance(e, PermissionError):
        return "PermissionError: O arquivo está sendo usado ou você não tem permissão para acessá-lo. Por favor, feche o arquivo."
    elif isinstance(e, TypeError):
        return f"TypeError: Erro de tipo. Verifique se os dados são do tipo correto. Mensagem original: {e}"
    elif isinstance(e, ValueError):
        return f"ValueError: Erro de valor. Mensagem original: {e}"
    else:
        return f"Ocorreu um erro inesperado: {e}"  
def ajuste_col(df_original, col):
            df_copia = df_original.copy() 
            for i in col:
                df_copia[i] = df_copia[i].fillna(0).astype(str)
                df_copia[i] = df_copia[i].str.replace(".", "", regex=False)
                df_copia[i] = df_copia[i].str.replace(",", ".")
                df_copia[i] = df_copia[i].fillna(0).astype(float)
            return df_copia
def correcao(df, dt):
    var = df.copy()

    var['dia'] = var[dt].dt.day_name('pt_BR')
    var['mes'] = var[dt].dt.month_name('pt_BR')
    var['ano'] = var[dt].dt.year
    var[dt] = var[dt].dt.strftime("%d-%m-%Y")     
    var['vl_corte'] = var['vl_corte'].round(2).astype(str).str.replace('.', ',', regex= False)
    var = var.sort_values(by=dt, ascending= True, axis= 0)
    return var

def app():
    
    try: # Extração dos dados nessecario 
        files = glob.glob(os.path.join(pasta.p_41, '*.xls*'))
        df_corte = pd.read_csv(ar_csv.ar_67,header= None, names= col_name.c67)
        df_consolidado = pd.read_excel(ar_xlsx.acum_41, sheet_name= 'acumulado_ped')
    except Exception as e:
        error = validar_erro(e)
        print(F"Extração: {error}")
    
    try:
            try:
                df_corte['hora'] = df_corte['hr'].astype(str) + ':' + df_corte['min'].astype(str)
                df_corte['data'] = pd.to_datetime(df_corte['data'], format= '%d/%m/%y').sort_index(axis= 0, ascending= True)
                df_corte['hora'] = pd.to_datetime(df_corte['hora'], format= '%H:%M').dt.strftime('%H:%M')

                col_ajustar = ['vl_corte', 'qtde_corte']
                df_corte = ajuste_col(df_corte, col_ajustar)
                df_corte = df_corte.sort_values(by="data", ascending= True, axis= 0)

            except Exception as e:
                error = validar_erro(e)
                print(F"\nCortes geral: {error}")

            try:
                df_dia = df_corte.loc[df_corte['hora'].between("07:30:00", "18:00:00")].copy()

                var_dia = df_dia.groupby('data').agg(
                            vl_corte=('vl_corte', 'sum'),
                            qtde_corte=('qtde_corte', 'count'),
                            qtde_item=('desc', 'nunique')
                        ).reset_index()
                var_dia = correcao(var_dia, 'data')
                       
            except Exception as e:
                error = validar_erro(e)
                print(F"\nCortes dias: {error}")

            try:
                df_noite = df_corte.loc[~df_corte['hora'].between("07:30:00", "18:00:00")].copy()
                df_noite['data'] = pd.to_datetime(df_noite['data'], format= '%d/%m/%Y')
                df_noite["data_turno"] = df_noite['data'].copy()
                df_noite.loc[df_noite['hora'] < "07:30:00", 'data_turno'] -= pd.Timedelta(days=1)
                var_noite = df_noite.groupby('data_turno').agg(
                    vl_corte=('vl_corte', 'sum'),
                    qtde_corte=('qtde_corte', 'count'),
                    qtde_item=('desc', 'nunique')
                ).reset_index()
                var_noite = correcao(var_noite, 'data_turno')
                
            except Exception as e:
                error = validar_erro(e)
                print(F"\nCorte noite: {error}")

            try:
                total_files = len(files)
                lista_name = []
                lista_files = []
                if total_files == 0:
                    print("Nenhum arquivo Excel encontrado na pasta especificada.")
                    return None
                
                for file in files:
                    name_file = os.path.basename(file)
                    print(name_file)
                    valor_existe = (df_consolidado['Nome arquivo'] == name_file).any()
                    if valor_existe:
                        continue
                    lista_name.append(name_file)

                    try:
                        df_var = pd.read_excel(file)

                        if 'NUMPED' not in df_var.columns:
                            print(f"   AVISO: Coluna 'NUMPED' não encontrada no arquivo {name_file}\n")
                            continue
                        pedido = df_var['NUMPED'].nunique()
                        produto = df_var['PRODUTO'].nunique()

                        df_pedidos = pd.DataFrame({
                            "Nome arquivo" :  [name_file]
                            ,"Qtde_pedido"  : [pedido]
                            ,"Qtde_produto" : [produto]
                        })
                        lista_files.append(df_pedidos)
                        tt = len(lista_name)
                        print(f"arquivos: \n{tt}")

                    except Exception as e:
                        error = validar_erro(e)
                        print(F"Extração: {error}")
                        continue
                
                df_consolidado = pd.concat(lista_files, ignore_index= True)
                df_consolidado = df_consolidado.drop_duplicates(subset= None, keep= 'first')
                df_consolidado['DATA_BRUTA'] = df_consolidado['Nome arquivo'].str.extract(r'(\d{2}[-_]\d{2})')
                df_consolidado['DATA_COMPLETA'] = df_consolidado['DATA_BRUTA'].str.replace('_', '-') + '-2025'
                df_consolidado['DATA'] = pd.to_datetime(df_consolidado['DATA_COMPLETA'], format='%d-%m-%Y')
                df_consolidado.drop(columns=['DATA_BRUTA', 'DATA_COMPLETA'], inplace=True)
            except Exception as e:
                error = validar_erro(e)
                print(F"\nPedidos | divergencia: {error}")
    except Exception as e:
        error = validar_erro(e)
        print(F"\nTratamento: {error}")

    try:
        print("="*60)
        print("\nRelatorio de corte\n")
        print(f"DIA:\n {var_dia}")
        print(f"NOITE:\n {var_noite}")

        max_ex_dia= df_dia['data'].max() 
        max_ex_noite = df_noite['data_turno'].max()

        ex_dia = df_dia.loc[df_dia['data'] == max_ex_dia]
        ex_noite = df_noite.loc[df_noite['data_turno'] == max_ex_noite]

        with pd.ExcelWriter(output.corte) as writer:
            df_corte.to_excel(writer, sheet_name='extrato', index= False)
            ex_dia.to_excel(writer, sheet_name= 'ex_dia', index= False)
            ex_noite.to_excel(writer, sheet_name= 'ex_noite', index= False)
            df_consolidado.to_excel(writer, sheet_name= 'acumulado_ped', index= False)
            print('\nfim do processo, verifique o arquivo controle_corte.xlsx')
    except Exception as e:
        error = validar_erro(e)
        print(F"Carga: {error}")

if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")
