import settings
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    print(':: Iniciando migração do sistema Compras/Contratos')
    params_exec = {
        # 'clicodigo': '2016',  # PM
        # 'clicodigo': '13482',  # SAUDE
        # 'clicodigo': '16975',  # FAMABI
        'clicodigo': '11968',  # CAMARA
        'ano': 2018,
        'somente_pre_validar': False,
        'id_contratacao': ' and id_contratacao in (960483, 960482, 960445, 960446, 960449, 960459, 960436, 960458, 960457, 960491, 960443, 960447, 960497, 960498, 960441, 960442, 960500, 960435, 960440, 960439, 960437, 960438, 960434, 960493, 960495, 960496, 960494, 960499, 960486, 960492, 960490, 960489, 960485, 960488, 960479, 960487, 960480, 960484, 960481, 960478, 960475, 960476, 960474, 960477, 960466, 960472, 960471, 960469, 960473, 960468, 960470, 960467, 960461, 960465, 960464, 960460, 960462, 960463)',
        'token': '86e6501b-59b7-4793-874c-bee84100e779'  # oficial: Camara #ignoreline,

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
    enviar(params_exec, 'processo-item')
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
    # enviar(params_exec, 'contratacao-item')
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
