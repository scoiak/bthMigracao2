import settings
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    print(':: Iniciando migração do sistema Compras/Contratos')
    params_exec = {
        'clicodigo': '2016',
        'ano': 2015,
        'somente_pre_validar': False,
        'token': '55d73a4b-cc32-4da2-9734-854b28f50196' #ignoreline base de teste
        # 'token': 'c420e8c5-bc8a-44c4-8b34-e46df7867a3e' #oficial:Município de Biguaçu
    }
    mensagem_inicio(params_exec)
    interacao_cloud.verifica_token(params_exec['token'])
    verifica_tabelas_controle()

    # Envio Compras
    # enviar(params_exec, 'configuracoes-organogramas')
    # enviar(params_exec, 'orgao')
    # enviar(params_exec, 'unidade')
    # enviar(params_exec, 'centro-custo')
    # enviar(params_exec, 'parametro-exercicio')
    # enviar(params_exec, 'parametro-exercicio-compras')
    # enviar(params_exec, 'forma-julgamento')
    # enviar(params_exec, 'grupo')
    # enviar(params_exec, 'classe')
    # enviar(params_exec, 'unidade-medida')
    # enviar(params_exec, 'material')
    # enviar(params_exec, 'material-especificacao')
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
    # enviar(params_exec, 'solicitacao')
    # enviar(params_exec, 'solicitacao-item')
    # enviar(params_exec, 'solicitacao-despesa')
    # enviar(params_exec, 'tipo-sessao-julgamento')
    # enviar(params_exec, 'natureza-juridica')
    # enviar(params_exec, 'fundamento-legal')
    # enviar(params_exec, 'responsavel')
    # enviar(params_exec, 'fornecedor')
    # enviar(params_exec, 'comissao')
    # enviar(params_exec, 'processo')
    # enviar(params_exec, 'processo-documento')
    # enviar(params_exec, 'processo-entidade')
    # enviar(params_exec, 'processo-despesa')
    enviar(params_exec, 'processo-item')
    # Envio Contratos


def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do assunto {tipo_registro}')
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
