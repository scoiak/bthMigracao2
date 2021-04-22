import settings
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    print(':: Iniciando migração do sistema Compras/Contratos')
    params_exec = {
        # 'clicodigo': '2016',  # PM
        'clicodigo': '13482',  # SAUDE
        # 'clicodigo': '16975',  # FAMABI
        # 'clicodigo': '11968',  # CAMARA
        'ano': 2017,
        'somente_pre_validar': False,
        'id_contratacao': ' and id_contratacao in (907555,907734,908571,908489,908497,908515,908516,908703,907601,907605,908742,908743,908744,908745,907629,907633,907638,908750,907680,907682,907704,907708,907712,907716,907720,908754,908755,908756,908757,908758,908759,908760,908761,908762,908763,907724,907728,907746,907750,907754,908751,908764,908765,908766,908767,907880,907883,907887,907921,908792,908793,908794,908795,908796,907891,908797,907925,907929,907933,907936,907943,907947,907951,907965,907969,908799,908800,908801,908802,908803,908804,908805,908806,908807,907973,907977,907981,907987,907991,907995,908808,907999,908003,908007,908012,908016,908020,908024,908809,908028,908039,908043,908810,908811,908812,908100,908104,908824,908825,908740,908741,908752,908753,908769,908770,908771,908772,908773,908774,908775,908776,908777,908778,908779,908780,908781,908782,908783,908784,908785,908786,908787,908788,908790,908791,908814,908815,908816,908817,908818,908819,908820,908821,908823,908867,869077,890001,869111,890002,890003,908960,908961,908962,908963,908964,908965,908966,908967,908968,908969,908970,908971,908972,908973,908974,908975,908976,908983,908984,908988,908989,908990,908991,908993,908994,908995,908996,908997,908998,908999,909000,909001,909002,909003,909004,909005,909006,909009,869020,869068,869074,869075,869076,869078,869079,869080,869081,869082,869083,869084,869085,869086,869087,869089,869090,869091,869092,869093,869094,869095,869096,869097,869098,869099,869100,869101,869102,869103,869104,869105,869106,869107,869108,869109,869110,879007,869007,869018,869021,869034,869035,869036,869037,911728,868986,868987,868988,868994,878867,878868,860019)',
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
