from OUTROS.path_arquivos import *
import pandas as pd
import numpy as np
import glob
import os 
import warnings
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

def app():
    def ajuste_col(df_original, col):
            df_copia = df_original.copy() 
            for i in col:
                df_copia[i] = df_copia[i].fillna(0).astype(str)
                df_copia[i] = df_copia[i].str.replace(".", "", regex=False)
                df_copia[i] = df_copia[i].str.replace(",", ".")
                df_copia[i] = df_copia[i].fillna(0).astype(float)
            return df_copia
    
    try: # Extração dos dados nessecario 
        files = glob.glob(os.path.join(pasta.p_41, '*.xls*'))
        df_corte = pd.read_csv(ar_csv.ar_67,header= None, names= col_name.c67)
        df_consolidado = pd.read_excel(output.corte, sheet_name= 'acumulado_ped')
    except Exception as e:
        error = validar_erro(e)
        print(F"Extração: {error}")
    
    try:
            try:
                print("\n")
                print("="*60)
                print("\nIniciando contagem de pedidos, aguarde...\n")
                total_files = len(files)
                lista_name = []
                lista_files = []
                if total_files == 0:
                    print("Nenhum arquivo Excel encontrado na pasta especificada.")
                    return None
                
                for file in files:
                    name_file = os.path.basename(file)
                    lista_name.append(name_file)
                    
                    df_var = pd.read_excel(file)
                    if 'NUMPED' not in df_var.columns:
                        print(f"   AVISO: Coluna 'NUMPED' não encontrada no arquivo {name_file}\n")
                        continue
                    pedido = df_var['NUMPED'].nunique()
                    produto = df_var['PRODUTO'].nunique()

                    df_pedidos = pd.DataFrame({
                        "Nome arquivo" : [name_file],
                        "Qtde_pedido"  : [pedido],
                        "Qtde_produto" : [produto]
                    })
                    lista_files.append(df_pedidos)
                    tt = len(lista_name)
                    print(f"arquivos: \n{tt}")
                
                df_consolidado = pd.concat(lista_files, ignore_index= True)
                df_consolidado = df_consolidado.drop_duplicates(subset= None, keep= 'first')
                
                print(f"lista de arquivos: \n{tt}")
                
            except Exception as e:
                error = validar_erro(e)
                print(F"Pedidos | divergencia: {error}")

            try:
                pass
            except Exception as e:
                error = validar_erro(e)
                print(F"Cortes dias: {error}")

            try:
                with pd.ExcelWriter(output.corte) as writer:
                    # .to_excel(writer, sheet_name='extrato', index= False)
                    # .to_excel(writer, sheet_name= 'ex_dia', index= False)
                    # .to_excel(writer, sheet_name= 'ex_noite', index= False)
                    df_consolidado.to_excel(writer, sheet_name= 'acumulado_ped', index= False)
                    print('fim do processo, verifique o arquivo controle_corte.xlsx')
            except Exception as e:
                error = validar_erro(e)
                print(F"Corte noite: {error}")


    except Exception as e:
        error = validar_erro(e)
        print(F"Tratamento: {error}")

    try:

        pass

    except Exception as e:
        error = validar_erro(e)
        print(F"Carga: {error}")


if __name__ == "__main__":
    app()
    input("\nPressione Enter para sair...")
