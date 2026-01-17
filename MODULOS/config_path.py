import os
import sys

if getattr(sys, 'frozen', False):
    # Se for um executável .exe
    BASE_DIR = os.path.dirname(sys.executable)
    INTERNAL_DIR = sys._MEIPASS
else:
    PASTA_ATUAL = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = os.path.dirname(PASTA_ATUAL) 
    INTERNAL_DIR = BASE_DIR

PASTA_DADOS = os.path.join(BASE_DIR, "dados_files")
FILE_RETORNO = os.path.join(BASE_DIR, 'RESULTADO_FINAL.xlsx')
PASTA_STYLE = os.path.join(BASE_DIR, "styles")

class Path_dados:
    icone_pricipal = os.path.join(PASTA_STYLE, 'flesh_perfil.ico')
    icone_segundario = os.path.join(PASTA_STYLE, 'sloth_icon.ico')
    png_jc = os.path.join(PASTA_STYLE, 'JC_Distribuição.png')
    
class DB_acumulado:
    _fixar = os.path.join(PASTA_DADOS, 'BASE_FIXAR')
    path_acumulado = os.path.join(_fixar, "DB_ACUMULADO.accdb")
    drive = '{Microsoft Access Driver (*.mdb, *.accdb)}'

    tb_vz_ch = "DB_VAZIO_CHEIO"
    db_pedidos = "DB_PEDIDOS"
    db_prod = "DB_INVPROD"
    db_cont = "DB_INVCONT"


class Relatorios:
    rel_41 = os.path.join(PASTA_DADOS, '8041 - Relatorio de picking master.xlsx')
    rel_32 = os.path.join(PASTA_DADOS, '8132 - Relatorio de endereçamento.xlsx')
    rel_34 = os.path.join(PASTA_DADOS, '8134 - Analitico avaria.xlsx')
    rel_60 = os.path.join(PASTA_DADOS, '8560 - Relatório acesso.xlsx')
    rel_73 = os.path.join(PASTA_DADOS, '8573 - Emissão etiquetas cod. barras.xlsx')
    rel_76 = os.path.join(PASTA_DADOS, '8576 - Acompanhamento de anomalias.xlsx')
    rel_96 = os.path.join(PASTA_DADOS, '8596 - Dados logistico produtos WMS.xlsx')
    rel_11 = os.path.join(PASTA_DADOS, '8611 - Responsável por gerar o.xlsx')
    rel_28 = os.path.join(PASTA_DADOS, '8628 - Relatorio de abastecimento.xlsx')
    rel_64 = os.path.join(PASTA_DADOS, '8664 - Eficiência do recebimento.xlsx')
    
    rel_f18 = os.path.join(PASTA_DADOS, '8596 - Dados filial_18.xlsx')
class Wms:
    wms_02     = os.path.join(PASTA_DADOS, '1702 - Cadastro de Endereços.txt')
    wms_07_ger = os.path.join(PASTA_DADOS, '1707 - Estoque endereçado x Gerencial.txt')
    wms_07_end = os.path.join(PASTA_DADOS, '1707 - Relatório de produtos por endereços.txt')
    wms_33     = os.path.join(PASTA_DADOS, '1733 - Gestão de inventario.xls')
    wms_67     = os.path.join(PASTA_DADOS, '1767 - Relatorio de corte.txt')
    wms_82     = os.path.join(PASTA_DADOS, '1782 - Relatório de curva ABC de produtos.xls')

class Outros:
    ou_corte = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\6.1.1 Analise de Cortes\2026\# 2 Acompanhamento de produtos cortados.xlsx'
    
    _fixar = os.path.join(PASTA_DADOS, 'BASE_FIXAR')
    ou_end   = os.path.join(_fixar, 'BASE_ENDEREÇOS.xlsx')
    ou_func  = os.path.join(_fixar, 'BASE_FUNCIONARIO.xlsx')
    
    ou_life  = os.path.join(PASTA_DADOS, '001 - TOTVS_LIFE.xlsx')
    ou_18    = os.path.join(PASTA_DADOS, '1118 - Gerencial 11.xls')
    ou_86    = os.path.join(PASTA_DADOS, '0286 - Consultar Produtos.xls')
    ou_f18   = os.path.join(PASTA_DADOS, '0286 - Consultar filial_18.xls')
    ou_05    = os.path.join(PASTA_DADOS, '0105 - Posição do estoque.xls')
    ou_718   = os.path.join(PASTA_DADOS, '0718 - Contas pagas.xls')
    
class Directory:
    dir_cheio_vazio = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\6.1.5 - Relatório Cheio x vazio\RETORNO'

    # Diretórios dentro da Base de Dados
    dir_PROD    = os.path.join(PASTA_DADOS, 'BASE_1733', 'INV_PROD')
    dir_CONT     = os.path.join(PASTA_DADOS, 'BASE_1733', "INV_CONT")
    dir_acum    = os.path.join(PASTA_DADOS, 'ACUMULADOS')
    dir_82      = os.path.join(PASTA_DADOS, 'BASE_1782')
    dir_41      = os.path.join(PASTA_DADOS, 'BASE_8041')
    dir_28      = os.path.join(PASTA_DADOS, 'BASE_8628')
    dir_64      = os.path.join(PASTA_DADOS, 'BASE_8664')
    dir_fixar   = os.path.join(PASTA_DADOS, 'BASE_FIXAR')

    # Diretórios de Output (BI)
    _out  = os.path.join(BASE_DIR, "OUTPUT")
    dir_acuracidade = os.path.join(_out, 'BI_ACURACIDADE')
    dir_acesso      = os.path.join(_out, 'BI_ACESSO')

    @classmethod
    def criar_pastas(cls):
        """Cria as pastas de saída automaticamente se não existirem"""
        pastas = [cls.dir_acuracidade, cls.dir_acesso, cls.dir_acum]
        for pasta in pastas:
            if not os.path.exists(pasta):
                os.makedirs(pasta)
                print(f"Pasta criada: {pasta}")

class Output:
    _base = os.path.join(BASE_DIR, "OUTPUT")
    _acum = os.path.join(PASTA_DADOS, "ACUMULADOS")

    inv          = os.path.join(_base, 'analitico_contagem.xlsx')
    life         = os.path.join(_base, 'TOTVS_LIFES.xlsx')
    controle_fl  = os.path.join(_base, 'Giro_Status.xlsx')
    rel_os       = os.path.join(_base, 'analitico_baixa.xlsx')
    cadastro     = os.path.join(_base, 'analitico_cadastro.xlsx')
    cheio_vazio  = os.path.join(_base, 'geral_cheio_vazio.xlsx')
    corte        = os.path.join(_base, 'extratos_corte.xlsx')
    acum_41      = os.path.join(_acum, 'ACUMULADO_41.xlsx')
class Power_BI:
    _base_dados = PASTA_DADOS
    _out = os.path.join(BASE_DIR, "OUTPUT")
    
    # Relatório de Abastecimentos (Fontes de entrada)
    abst_atual64 = os.path.join(_base_dados, 'BASE_8664', '01- JANEIRO 8664.xlsx')
    abst_atual28 = os.path.join(_base_dados, 'BASE_8628', '8628 - 01 - JANEIRO.xlsx')

    # Relatório de Abastecimentos (Saídas/Outputs)
    _aba = os.path.join(_out, 'BI_ABASTECIMENTO')
    abst_bonus  = os.path.join(_aba, 'BONUS_CONT.xlsx')
    abst_fim    = os.path.join(_aba, 'OS_FIM.xlsx')
    abst_pd     = os.path.join(_aba, 'OS_PD.xlsx')
    abst_geral  = os.path.join(_aba, 'OS_GERAL.xlsx')
    abst_cons64 = os.path.join(_aba, 'CONSOLIDADA_8664.xlsx')
    abst_cons28 = os.path.join(_aba, 'CONSOLIDADA_8628.xlsx')

    # Relatório de Acuracidade (Saídas)
    _acu = os.path.join(_out, 'BI_ACURACIDADE')
    acu_prod  = os.path.join(_acu, 'DIM_PROD.xlsx')
    acu_diver = os.path.join(_acu, 'FATO_DIVERGENCIA.xlsx')
    acu_inv   = os.path.join(_acu, 'FATO_INVENTARIO.xlsx')

    # Relatório de Acesso (Saídas)
    _ace = os.path.join(_out, 'BI_ACESSO')
    ace_ace  = os.path.join(_ace, 'DIM_ACESSO.xlsx')
    ace_st   = os.path.join(_ace, 'DIM_ESTOQUE.xlsx')
    ace_mov  = os.path.join(_ace, 'DIM_MOVIMENTAR.xlsx')
    ace_prod = os.path.join(_ace, 'DIM_PRODUTO.xlsx')
    ace_sug  = os.path.join(_ace, 'DIM_SUGESTÃO.xlsx')
    ace_fato = os.path.join(_ace, 'DIM_SUGESTÃO.xlsx')

    # Relatório de Cheio x Vazio (Histórico/Acumulado)
    ch_vz = os.path.join(_base_dados, 'ACUMULADOS', 'acumulado_cheio_vazio.xlsx')
class Ipynb:
    _base = os.path.join(BASE_DIR, "OUTPUT")
    # Salvando os resultados na raiz do projeto (BASE_DIR)
    retorno   = os.path.join(_base, 'RESULTADO.xlsx')
    retorno_1 = os.path.join(_base, 'RESULTADO_FINAL.xlsx')

class ColNames:
    # Listas de colunas não precisam de caminhos, então apenas mantemos a estrutura
    col_67  = ['data', 'n_car', 'cod', 'desc', 'n_ped', 'qtde_orig', 'vl_orig', 'rua', 'predio', 'apto', 'estoque_dia', 'qtde_corte', 'vl_corte', 'hr', 'min', 'motivo', 'cod_fuc', 'func']
    col_ger = ['COD', 'DESCRICAO', 'DEP', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'EMBALAGEM', 'CAP', 'P_REP', 'QTDE_O.S', 'PICKING', 'PULMAO', 'ENDERECO', 'GERENCIAL']
    col_end = ['COD_END', 'PREDIO','NIVEL', 'APTO', 'STATUS', 'COD', 'DESC', 'DT_ENTRADA', 'DT_VALIDADE','EMB', 'LASTRO', 'CAMADA', 'CAP', 'TIPO_PK','PL_END', 'QTDE', 'ENTRADA', 'SAIDA', 'DISP']
    col_mov = ['COD', 'DESC', 'EMBALAGEM', 'UNID', 'MOVI', '%', '%_ACUM', 'CLASSE']
    col_sug = ['COD', 'DESCRIÇÃO', 'EMBALAGEM', 'UNID.', 'DEP', 'RUA', 'PREDIO', 'NIVEL', 'APTO', 'MÊS 1', 'MÊS 2', 'MÊS 3', 'TIPO', 'CAP', '1 DIA', 'COM FATOR', 'VARIAÇÃO', '%']
    col_02  = ['FL','COD_END','DEP','RUA','PREDIO','NIVEL','APTO','TIPO','ROTATIVO','BLOQ','DISP.','PALETE','ESTRUTURA']