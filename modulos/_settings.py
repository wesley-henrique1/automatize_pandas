import sys
import os 

class aux:
    if getattr(sys, "frozen", False):
        Diretorio = os.path.dirname(sys.executable)
        Estatico = sys._MEIPASS
    else:
        PastaAtual = os.path.dirname(os.path.abspath(__file__))
        Diretorio = os.path.dirname(PastaAtual)
        Estatico = Diretorio

    OutPut = os.path.join(Diretorio, "ARQUIVOS_GERADOS")
    assets = os.path.join(Diretorio, "Assets")
    DataBase = os.path.join(Diretorio, "DataBase")
    Interface = os.path.join(Diretorio, "interface")
    Modulos = os.path.join(Diretorio, "modulos")

class Assets(aux):
    Icons = os.path.join(aux.assets, "icons")
    Jsons = os.path.join(aux.assets, "jsons")

    corte = os.path.join(Icons, "corte_img.ico")
    Flesh = os.path.join(Icons, 'flesh_completo.png')
    FleshIcon = os.path.join(Icons, 'flesh_perfil.ico')

    Jornada = os.path.join(Jsons, "jornada.json")
    LogTime = os.path.join(Jsons, 'logTime.json')
    Rotinas = os.path.join(Jsons, "rotinas.json")

    pass
class Filial_18(aux):
    _8596 = os.path.join(aux.DataBase, '8596 - Dados filial_18.xlsx')
    _286 = os.path.join(aux.DataBase, '0286 - Consultar filial_18.xls')
    
    pass
class Gestao(aux):
    Corte = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\6.1.1 Analise de Cortes\2026\# 2 Acompanhamento de produtos cortados.xlsx'
    CheioVazio = r'z:\1 - CD Dia\4 - Equipe PCL\6.1 - Inteligência Logística\6.1.5 - Relatório Cheio x vazio\RETORNO'


    _001 = os.path.join(aux.DataBase, '001 - TOTVS_LIFE.xlsx')
    _105 = os.path.join(aux.DataBase, '0105 - Posição do estoque.xls')
    _286 = os.path.join(aux.DataBase, '0286 - Consultar Produtos.xls')
    _718 = os.path.join(aux.DataBase, '0718 - Contas pagas.xls')
    _1118 = os.path.join(aux.DataBase, '1118 - Gerencial 11.xls')

    MalhaFina_day = os.path.join(aux.DataBase, 'CD MATRIZ - Malha Fina 112025.xlsx')
    MalhaFina_mes = os.path.join(aux.DataBase, 'CD MATRIZ - Malha Fina até 6 meses.xlsx')
    pass
class Relatorios(aux):
    _8041 = os.path.join(aux.DataBase, '8041 - Relatorio de picking master.xlsx')
    _8132 = os.path.join(aux.DataBase, '8132 - Relatorio de endereçamento.xlsx')
    _8134 = os.path.join(aux.DataBase, '8134 - Analitico avaria.xlsx')
    _8560 = os.path.join(aux.DataBase, '8560 - Relatório acesso.xlsx')
    _8573 = os.path.join(aux.DataBase, '8573 - Emissão etiquetas cod. barras.xlsx')
    _8576 = os.path.join(aux.DataBase, '8576 - Acompanhamento de anomalias.xlsx')
    _8596 = os.path.join(aux.DataBase, '8596 - Dados logistico produtos WMS.xlsx')
    _8611 = os.path.join(aux.DataBase, '8611 - Responsável por gerar o.xlsx')
    _8628 = os.path.join(aux.DataBase, '8628 - Relatorio de abastecimento.xlsx')
    _8664 = os.path.join(aux.DataBase, '8664 - Eficiência do recebimento.xlsx')
    _8668 = os.path.join(aux.DataBase, '8668 - Produtos Aereo x Picking.xlsx')

    pass
class Wms(aux):
    _1702 = os.path.join(aux.DataBase, '1702 - Cadastro de Endereços.txt')
    _1733 = os.path.join(aux.DataBase, '1733 - Gestão de inventario.xls')
    _1767 = os.path.join(aux.DataBase, '1767 - Relatorio de corte.txt')
    _1782 = os.path.join(aux.DataBase, '1782 - Relatório de curva ABC de produtos.xls')

    validade07 = os.path.join(aux.DataBase, '1707 - Consultar Validade.csv')
    gerencial07 = os.path.join(aux.DataBase, '1707 - Estoque endereçado x Gerencial.txt')
    endereco07 = os.path.join(aux.DataBase, '1707 - Relatório de produtos por endereços.txt')
    geral07 = os.path.join(aux.DataBase, '1707 - Relatório geral.txt')

    pass
class BaseDados(aux):
    _1782 = os.path.join(aux.DataBase, 'BASE_1782')
    _8041 = os.path.join(aux.DataBase, 'BASE_8041')
    _8668 = os.path.join(aux.DataBase, 'BASE_8668')
    BaseFixa = os.path.join(aux.DataBase, 'BASE_FIXAR')

    EndFixo = os.path.join(BaseFixa, 'BASE_ENDEREÇOS.xlsx')
    Func = os.path.join(BaseFixa, 'BASE_FUNCIONARIO.xlsx')

    path_acumulado = os.path.join(BaseFixa, "DB_ACUMULADO.accdb")
    drive = '{Microsoft Access Driver (*.mdb, *.accdb)}'

    DBVzCh = "DB_VAZIO_CHEIO"
    DBPedidos = "DB_PEDIDOS"
    DBProd = "DB_INVPROD"
    DBCont = "DB_INVCONT"
    pass

class OutPut(aux):
    Baixas = os.path.join(aux.OutPut, 'Analitico baixa.xlsx')
    Cadastro = os.path.join(aux.OutPut, 'Analitico cadastro.xlsx')
    Corte = os.path.join(aux.OutPut, 'Analitico corte.xlsx')
    GiroStatus = os.path.join(aux.OutPut, 'Giro_Status.xlsx')
    MapaEstoque = os.path.join(aux.OutPut, 'mapa_estoque.xlsx')

    Acuracidade = os.path.join(aux.OutPut, 'BI_Acuracidade.xlsx')
    Abastecimento = os.path.join(aux.OutPut, 'BI_ABASTECIMENTO.xlsx')
    
    Jupyter_1 = os.path.join(aux.OutPut, 'JUPYTER_1')
    Jupyter_2 = os.path.join(aux.OutPut, 'JUPYTER_2')

    P_Acuracidade = os.path.join(aux.OutPut, 'BI_ACURACIDADE')

    Fefo8668 = r"z:\1 - CD Dia\4 - Equipe PCL\6.6 - Recuperação e Indenizado\6.6.3 - FEFO Validade\Curva A-B-C-D\Auditoria\FEFO_8668.xlsx"
    Fefo8628 = r"z:\1 - CD Dia\4 - Equipe PCL\6.6 - Recuperação e Indenizado\6.6.3 - FEFO Validade\Curva A-B-C-D\Auditoria\FEFO_8628.xlsx"

    pass
class ColNames():
    _1767 = [
        'data'
        ,'n_car'
        ,'cod'
        ,'desc'
        ,'n_ped'
        ,'qtde_orig'
        ,'vl_orig'
        ,'rua'
        ,'predio'
        ,'apto'
        ,'estoque_dia'
        ,'qtde_corte'
        ,'vl_corte'
        ,'hr'
        ,'min'
        ,'motivo'
        ,'cod_fuc'
        ,'func'
    ]
    _0702 = [
        'FL'
        ,'COD_END'
        ,'DEP'
        ,'RUA'
        ,'PREDIO'
        ,'NIVEL'
        ,'APTO'
        ,'TIPO'
        ,'ROTATIVO'
        ,'BLOQ'
        ,'DISP.'
        ,'PALETE'
        ,'ESTRUTURA'
    ]
    Geral = [
        "COD_END"
        ,"RUA"
        ,"PREDIO"
        ,"NIVEL"
        ,"APTO"
        ,"STATUS"
        ,"CODPROD"
        ,"DESCRICAO"
        ,"DT_VALIDADE"
        ,"EMBALAGEM"
        ,"LASTRO"
        ,"CAMADA"
        ,"NORMA_PL"
        ,"CAP_"
        ,"SALTO"
        ,"REP_"
        ,"TIPO_END"
        ,"QTDE"
        ,"ENTRADA"
        ,"SAIDA"
        ,"DISP_"
    ]
    Gerencial = [
        'COD'
        ,'DESCRICAO'
        ,'DEP'
        ,'RUA'
        ,'PREDIO'
        ,'NIVEL'
        ,'APTO'
        ,'EMBALAGEM'
        ,'CAP'
        ,'P_REP'
        ,'QTDE_O.S'
        ,'PICKING'
        ,'PULMAO'
        ,'ENDERECO'
        ,'GERENCIAL'
    ]
    Endereco = [
        'COD_END'
        ,'PREDIO'
        ,'NIVEL'
        ,'APTO'
        ,'STATUS'
        ,'COD'
        ,'DESC'
        ,'DT_ENTRADA'
        ,'DT_VALIDADE'
        ,'EMB'
        ,'LASTRO'
        ,'CAMADA'
        ,'CAP'
        ,'TIPO_PK'
        ,'PL_END'
        ,'QTDE'
        ,'ENTRADA'
        ,'SAIDA'
        ,'DISP'
    ]
    Movimentar = [
        'COD'
        ,'DESC'
        ,'EMBALAGEM'
        ,'UNID'
        ,'MOVI'
        ,'%'
        ,'%_ACUM'
        ,'CLASSE'
    ]
    Sugestao = [
        'COD'
        ,'DESCRIÇÃO'
        ,'EMBALAGEM'
        ,'UNID.'
        ,'DEP'
        ,'RUA'
        ,'PREDIO'
        ,'NIVEL'
        ,'APTO'
        ,'MÊS 1'
        ,'MÊS 2'
        ,'MÊS 3'
        ,'TIPO'
        ,'CAP'
        ,'1 DIA'
        ,'COM FATOR'
        ,'VARIAÇÃO'
        ,'%'
    ]
    pass