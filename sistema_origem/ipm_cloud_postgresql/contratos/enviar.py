import settings
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    print(':: Iniciando migração do sistema Compras/Contratos')
    params_exec = {
        'clicodigo': '2016',  # PM
        # 'clicodigo': '13482',  # SAUDE
        # 'clicodigo': '16975',  # FAMABI
        # 'clicodigo': '11968',  # CAMARA
        'ano': 2015,
        'somente_pre_validar': False,
        'id_contratacao': ' and id_contratacao in (875448,875460,875463,875479,875532,875533,875534,875542,875574,875577,875584,875588,875589,875591,875593,876060,876061,876063,876064,876066,876067,876068,876069,875596,875598,875631,875645,875701,876000,876018,875602,875604,875607,875610,960436,875613,875615,875617,875620,875622,875625,875626,875633,875635,875638,875641,875644,875649,875650,875652,875654,875657,875659,875668,875671,875676,875677,875684,875687,875692,875694,875697,875700,960449,875703,875706,875708,875712,875716,875718,875721,875722,875725,875728,875730,875735,875733,875741,960450,960451,960452,960453,960454,960455,960456,875738,875743,875745,875748,875750,875752,875754,875758,875762,875764,875769,875770,875773,875775,875777,875780,875783,875797,875785,875787,875788,875789,875792,875795,875801,875802,875804,875806,875808,875811,875813,875823,960466,875816,875818,875819,875821,875826,875828,875830,875832,875834,875835,875838,875843,875845,875846,875849,875850,875853,875854,875857,875859,875861,875862,875868,875869,875872,875874,875877,875880,875881,875884,875887,875889,875892,875894,875896,875898,875899,875901,875904,875906,875903,875908,875909,875912,875913,875916,875917,875919,875921,875922,875923,875926,875927,875929,875930,875934,875939,875940,875942,875945,875946,875948,875949,875952,875954,875956,875958,875960,875962,875964,875966,875968,875970,875972,875973,875976,875977,875979,875983,875984,875986,875987,875989,875991,875992,875994,875996,875998,876008,960490,876002,876004,876005,876007,876010,876013,876029,876015,876017,876022,876024,876025,876028,960497,960498,876031,876034,876035,876037,876038,876040,876041,876043,876045,876046,876047,876048,876049,876050,876051,876052,876053,876055,876056,876057,876058,876059,876062,876065,876070,909083,909415,909416,909417,909418,909420,909421,909422,909423,909425,909426,909427,909428,909429,909431,909432,909433,909434,909435,909436,909437,909439,909445,909440,909441,909442,909443,909444,909446,909447,909448,909449,909450,909451,909452,909455,909456,909457,909458,909459,909461,909462,909463,909464,909465,909466,909468,909469,909470,909467,909471,909472,909473,909474,909476,909477,909478,909479,909475,909480,909483,909484,909485,909486,909487,909488,909489,909490,909491,909492,909493,909494,909495,909496,909497,909498,909500,909501,909502,909503,909504,909505,909506,909507,909508,909509,909510,909511,909512,909513,909515,909516,909517,909555,909557,909560,909561,909562,909563,909566,909568,909570,909571,909578,909580,909594,909600,909601,909602,909603,909608,909609,909621,909638,909639,909640,909641,909642,909656,909661,909665,909666,909667,909673,909674,909676,909677,909678,909679,909680,909682,909688,909689,909691,909692,909693,909695,909699,909701,909704,909709,909713,909714,909718,909719,909720,909722,909723,909724,909725,909727,909728,909729,909730,909739,909740,909741,909752,909753,909754,909755,909825,909828,909829,909830,909831,909832,909833,909834,909835,909836,909837,909838,909839,909840,909841,909842,909843,909844,909845,909846,909847,909848,909849,909850,909856,909862,909851,909852,909853,909854,909855,909857,909858,909859,909860,909861,909863,909864,909865,909866,909867,909868,909869,909870,909871,909872,909873,909874,909875,909876,909877,909878,909879,909880,909881,909882,909883,909884,909885,909886,909887,909888,909889,909890,909891,909892,909893,909894,909896,909897,909898,909899,909900,909901,909902,909903,909904,909906,909907,909908,909910,909911,909912,909913,909914,909915,909916,909921,909927,909928,909932,909933,909935,909937,909941,909942,909944,909945,909946,909947,909948,909949,909950,909951,909952,909953,909954,909955,909956,909957,909958,909959,909960,909961,909962,909964,909965,909966,909967,909968,909969,909970,909971,909972,909973,909976,909978,909979,909981,909982,909983,909989,909992,909993,909995,909996,909997,909998,910000,910001,910002,910003,910004,910005,910008,910009,910010,910011,910012,910013,910014,910015,910016,910017,910020,910021,910022,910023,910024,910025,910026,910027,910028,910029,910030,910034,910035,910037,910038,910039,910057,910058,910059,910060,910061,910062,910063,910064,910065,910066,910069,910070,910071,910072,910073,910076,910077,910078,910079,910080,910081,910082,910083,910084,910085,910086,910087,910089,910090,910091,910092,910093,910094,910095,910096,910098,910099,910100,910101,910102,910103,910104,910105,910106,910107,910108,910109,910111,910112,910113,910114,910116,910117,910118,910119,910120,910121,910122,910123,910124,910125,910126,910128,910129,910130,910131,910132,910134,910135,910136,910137,910138,910139,910145,910146,910149,910150,910151,910152,910153,910787,911010,911011,911012,911013,911014,911015,911016,911017,911018,911019,911020,911021,911022,911023,911024,911025,911165,911166,911167,911168,911169,911170,911171,911174,911285,911177,911182,911183,911185,911186,911190,911192,911194,911195,911197,911199,911200,911203,911204,911205,911206,911207,911208,911209,911233,911235,911242,911244,911245,911246,911248,911249,911251,911252,911253,911254,911255,911256,911283,911284,960434,960435,960437,960438,960439,960440,960441,960442,960443,960445,960446,960447,960457,960458,960459,960460,960461,960462,960463,960464,960465,960467,960468,960469,960470,960471,960472,960473,960474,960475,960476,960477,960478,960479,960480,960481,960482,960483,960484,960485,960486,960487,960488,960489,960491,960492,960493,960494,960495,960496,960499,960500)',
        # 'token': '86e6501b-59b7-4793-874c-bee84100e779'  # oficial: Camara #ignoreline,

    }
    mensagem_inicio(params_exec)
    interacao_cloud.verifica_token(params_exec['token'])
    verifica_tabelas_controle()

    """ Envio Compras """
    # enviar(params_exec, 'configuracoes-organogramas')
    # enviar(params_exec, 'orgao')
    # enviar(params_exec, 'unidade')
    # enviar(params_exec, 'centro-custo')
    # enviar(params_exec, 'parametro-exercicio')
    # enviar(params_exec, 'parametro-exercicio-compras')
    # enviar(params_exec, 'forma-julgamento')
    # enviar(params_exec, 'unidade-medida')
    # enviar(params_exec, 'grupo')
    # enviar(params_exec, 'classe')
    # enviar(params_exec, 'material')
    # enviar(params_exec, 'material-especificacao')
    # enviar(params_exec, 'material-duplicado-especificacao')
    # enviar(params_exec, 'tipo-objeto')
    # enviar(params_exec, 'regime-execucao')
    # enviar(params_exec, 'prazo-entrega')
    # enviar(params_exec, 'tipo-interposicao-recurso')
    # enviar(params_exec, 'tipo-publicacao')
    # enviar(params_exec, 'modalidade-licitacao')
    # enviar(params_exec, 'forma-pagamento')
    # enviar(params_exec, 'local-entrega')
    # enviar(params_exec, 'tipo-documento')
    # enviar(params_exec, 'pais')
    # enviar(params_exec, 'estado')
    # enviar(params_exec, 'municipio')
    # enviar(params_exec, 'bairro')
    # enviar(params_exec, 'tipo-logradouro')
    # enviar(params_exec, 'logradouro')
    # enviar(params_exec, 'cargo')
    # enviar(params_exec, 'tipo-ato')
    # enviar(params_exec, 'fonte-divulgacao')
    # enviar(params_exec, 'natureza-texto-juridico')
    # enviar(params_exec, 'ato')
    # enviar(params_exec, 'tipo-revogacao-anulacao')
    # enviar(params_exec, 'despesa')

    # Solicitações de Compra
    # enviar(params_exec, 'solicitacao')
    # enviar(params_exec, 'solicitacao-item')
    # enviar(params_exec, 'solicitacao-despesa')
    # enviar(params_exec, 'solicitacao-atualiza-status')
    # enviar(params_exec, 'tipo-sessao-julgamento')
    # enviar(params_exec, 'natureza-juridica')
    # enviar(params_exec, 'fundamento-legal')
    # enviar(params_exec, 'responsavel')
    # enviar(params_exec, 'fornecedor')
    # enviar(params_exec, 'comissao')
    # enviar(params_exec, 'comissao-membros')

    # Processos Administrativos
    # enviar(params_exec, 'processo')
    # enviar(params_exec, 'processo-forma-contratacao')
    # enviar(params_exec, 'processo-documento')
    # enviar(params_exec, 'processo-entidade')
    # enviar(params_exec, 'processo-despesa')
    # enviar(params_exec, 'processo-item')
    # enviar(params_exec, 'processo-lote')
    # enviar(params_exec, 'processo-lote-item')
    # # enviar(params_exec, 'processo-entidade-item')
    # enviar(params_exec, 'processo-convidado')
    # enviar(params_exec, 'processo-publicacao')
    # enviar(params _exec, 'processo-impugnacao')
    # enviar(params_exec, 'processo-sessao')
    # enviar(params_exec, 'processo-participante')
    # enviar(params_exec, 'processo-participante-documento')
    # enviar(params_exec, 'processo-participante-proposta')
    # enviar(params_exec, 'processo-proposta-pendente')
    # enviar(params_exec, 'processo-sessao-ata')
    # enviar(params_exec, 'processo-representante')
    # enviar(params_exec, 'processo-interposicao')
    # enviar(params_exec, 'processo-ato-final')
    # enviar(params_exec, 'processo-revogacao')

    # enviar(params_exec, 'processo-participante-proposta-busca')

    # Atas de Registro de Preço
    # enviar(params_exec, 'processo-item-configuracao')
    # enviar(params_exec, 'ata-rp')
    # enviar(params_exec, 'ata-rp-item')

    """ Envio Contratos """
    # enviar(params_exec, 'tipo-aditivo')
    # enviar(params_exec, 'tipo-administracao')
    # enviar(params_exec, 'tipo-sancao')
    # enviar(params_exec, 'tipo-responsavel-contrato')

    # Contratações - Compras Diretas
    # enviar(params_exec, 'compra-direta')
    # enviar(params_exec, 'compra-direta-item')
    # enviar(params_exec, 'compra-direta-despesa')
    # enviar(params_exec, 'compra-direta-sf')
    # enviar(params_exec, 'compra-direta-sf-item')

    # Contratações - Processos Administrativos
    # enviar(params_exec, 'contratacao')
    enviar(params_exec, 'contratacao-item')
    # enviar(params_exec, 'contratacao-aditivo')
    # enviar(params_exec, 'contratacao-aditivo-item') # Não utilizar
    # enviar(params_exec, 'contratacao-aditivo-item_v2') # Configurar antes de usar
    # enviar(params_exec, 'contratacao-apostilamento')
    # enviar(params_exec, 'contratacao-sf')
    # enviar(params_exec, 'contratacao-sf-item')
    # enviar(params_exec, 'contratacao-sf-recebimento')
    # enviar(params_exec, 'contratacao-sf-recebimento-item')
    # enviar(params_exec, 'contratacao-sf-recebimento-comprovante')

    # Contratações - Atas de Registro de Preço
    # enviar(params_exec, 'contratacao-arp')
    # enviar(params_exec, 'contratacao-arp-item')
    # enviar(params_exec, 'contratacao-arp-sf')
    # enviar(params_exec, 'contratacao-arp-sf-item')

    # Recebimentos e Comprovantes
    # enviar(params_exec, 'comprovante')
    # enviar(params_exec, 'solicitacao-recebimento')
    # enviar(params_exec, 'solicitacao-recebimento-item')
    # enviar(params_exec, 'solicitacao-recebimento-comprovante')

    """ Envio de Anexos """
    # enviar(params_exec, 'anexo') # !!! PENDENTE


def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do cadastro {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'sistema_origem.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)
    print(f'- Rotina de {tipo_registro} finalizada. '
          f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')


def mensagem_inicio(params_exec):
    print(f'\n:: Iniciando execução da migração do sistema {settings.BASE_ORIGEM} para Betha Cloud utilicando os '
          f'seguintes parâmetros: \n- {params_exec}')


def verifica_tabelas_controle():
    pgcnn = model.PostgreSQLConnection()
    pgcnn.verifica_tabelas_controle()
