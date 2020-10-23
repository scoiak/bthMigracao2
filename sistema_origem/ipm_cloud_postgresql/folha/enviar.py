import settings
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

def iniciar():
    print('\n:: Iniciando migração do sistema Folha')
    params_exec = {
        'somente_pre_validar': False,
        'token': '72612895-9758-467d-a2ef-36b8b57c3198'
    }
    mensagem_inicio(params_exec)
    interacao_cloud.verifica_token(params_exec['token'])
    verifica_tabelas_controle()
    # enviar(params_exec, 'paises')
    # enviar(params_exec, 'estados')
    # enviar(params_exec, 'cidades')
    # enviar(params_exec, 'bairro')
    # enviar(params_exec, 'tipo-logradouro')
    enviar(params_exec, 'logradouro')
    # enviar(params_exec, 'tipo-afastamento')
    # enviar(params_exec, 'bancos')
    # enviar(params_exec, 'agencia-bancaria')
    # enviar(params_exec, 'natureza-texto-juridico')
    # enviar(params_exec, 'motivo-alteracao-salarial')
    # enviar(params_exec, 'motivo-alteracao-cargo')
    # enviar(params_exec, 'tipo-ato')
    # enviar(params_exec, 'pessoa-fisica')

def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do assunto {tipo_registro}')
    path_padrao = f'sistema_origem.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)

def mensagem_inicio(params_exec):
    print(f':: Iniciando execução da migração do sistema {settings.BASE_ORIGEM} para Betha Cloud utilicando os '
          f'seguintes parâmetros: \n- {params_exec}')

def verifica_tabelas_controle():
    pgcnn = model.PostgreSQLConnection()
    pgcnn.verifica_tabelas_controle()