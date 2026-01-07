import pandas as pd
import datetime
import glob
import re
import os


class Funcao:
    def validar_erro(e):
        print("=" * 60)
        if isinstance(e, PermissionError):
            return "PermissionError: O arquivo está sendo usado ou você não tem permissão para acessá-lo. Por favor, feche o arquivo."
        elif isinstance(e, KeyError):
            return f"KeyError: A coluna ou chave '{e}' não foi encontrada."
        elif isinstance(e, TypeError):
            return f"TypeError: Erro de tipo. Verifique se os dados são do tipo correto. Mensagem original: {e}"
        elif isinstance(e, ValueError):
            return f"ValueError: Erro de valor. Mensagem original: {e}"
        elif isinstance(e, NameError):
            return f"NameError: Variável ou função não definida. Detalhe: {e}"
        else:
            return f"Ocorreu um erro inesperado: {e}"
 
    def leitura(path_files, contador):
        data_file = os.path.getmtime(path_files)
        data_modificacao = datetime.datetime.fromtimestamp(data_file)
        data_formatada = data_modificacao.strftime('%d/%m/%Y %H:%M:%S')
        nome_file = os.path.basename(path_files)
        files_completo = nome_file + " - " + data_formatada

        print(f"{contador} - {files_completo}\n")
       
    def extrair_e_converter_peso(argumento):
        match = re.search(r'([\d\.,]+)\s*(KG|GR)', str(argumento), re.IGNORECASE)
        if match:
            valor_str = match.group(1).replace(',', '.')
            valor = float(valor_str)
            unidade = match.group(2).upper()
            if unidade == 'KG':
                return valor * 1000
            elif unidade == 'GR':
                return valor     
        return None

    def ajustar_numero(df_copia, coluna, tipo_dados):
        def ajustar(valor):
            if pd.isna(valor) or valor is None:
                return 0.0
            valor = str(valor).strip()
            if isinstance(valor, str):
                if valor.count('.') >= 1 and ',' in valor: 
                    valor = valor.replace('.', '').replace(',', '.')
                elif ',' in valor:
                    valor = valor.replace(',', '.')
            try:
                if tipo_dados == int:
                    return int(float(valor))
                elif tipo_dados == float:
                    return round(float(valor), 2)
            except (ValueError, TypeError):
                return 0.0
        df_copia[coluna] = df_copia[coluna].apply(ajustar)
        return df_copia

    def director(files, argumento):
        director = glob.glob(os.path.join(files, argumento))
        lista = []
        for arquivo in director:    
            x = pd.read_csv(arquivo, header= None)
            lista.append(x)
        df_temp = pd.concat(lista, axis= 0, ignore_index= True)
        return df_temp
    
    def extrair_e_ordenar_data(df, data):
        var = df.copy()

        var['dia'] = var[data].dt.day_name('pt_BR')
        var['mes'] = var[data].dt.month_name('pt_BR')
        var['ano'] = var[data].dt.year
        var[data] = var[data].dt.strftime("%d-%m-%Y")    
        var = var.sort_values(by= data, ascending= True, axis= 0)
        return var
        

