import glob
import pandas as pd
import os
import warnings
import BASE as bs

# Desativa warnings indesejados
warnings.filterwarnings("ignore", category=UserWarning)

# Configuração do caminho
caminho_pasta = bs.path.p_8041_pasta
def processar_e_exibir():
    print("="*60)
    print("ANÁLISE DE CORTES - RELATÓRIO NO TERMINAL")
    print("="*60)
    print(f"Pasta analisada: {caminho_pasta}")
    print("-"*60)
    
    arquivos_excel = glob.glob(os.path.join(caminho_pasta, '*8.xls*'))
    total_arquivos = len(arquivos_excel)
    
    if total_arquivos == 0:
        print("Nenhum arquivo Excel encontrado na pasta especificada.")
        return
    
    print(f"\nEncontrados {total_arquivos} arquivos Excel:\n")
    
    for i, arquivo in enumerate(arquivos_excel, 1):
        nome_arquivo = os.path.basename(arquivo)
        print(f"{i}. Processando: {nome_arquivo}")
        
        try:
            df = pd.read_excel(arquivo)
            
            # Verifica colunas necessárias
            if 'NUMPED' not in df.columns:
                print("   AVISO: Coluna 'NUMPED' não encontrada\n")
                continue
                            
            cont_nunique = df['NUMPED'].nunique()
            cont = df['NUMPED'].count()

            group = df.groupby('RUA').agg(
                QTDE_ITENS = ('PRODUTO', 'count')
            ).reset_index()

            
            # Exibe resultados formatados
            print("\n   RESUMO:")
            print(f"   - Pedidos únicos: {cont_nunique}")
            print(f"   - Total de pedidos: {cont}")
            print("\nTOP 5 POR RUA:")
            print(group.sort_values(by='QTDE_ITENS', ascending= False).head(5))
            print("-"*60)
            
        except Exception as e:
            print(f"   ERRO: {str(e)}\n")
            continue

if __name__ == "__main__":
    processar_e_exibir()
    input("\nPressione Enter para sair...")