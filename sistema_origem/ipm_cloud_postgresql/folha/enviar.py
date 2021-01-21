from datetime import datetime
import settings
# import winsound
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

def iniciar():
    print(':: Iniciando migração do sistema Folha')
    params_exec = {
        'somente_pre_validar': False,                
        'enviar': True,
        'buscar': True,        
        'atualizar': False,
        # 'token': '72612895-9758-467d-a2ef-36b8b57c3198',  # PM - H - 1 #ignoreline
        # 'token': '58924393-e014-43f4-9269-5646a39b127d',  # PM - H - 2 #ignoreline
        # 'token': 'c52c4510-0a8f-468f-a501-1f68b32795c8',  # PM - H - 3 #ignoreline
        # 'token': 'c420e8c5-bc8a-44c4-8b34-e46df7867a3e',  # PM - O #ignoreline
        # 'token': '144e13ad-29ce-49b7-b9dc-7d95ee29b0f6',  # FAMABI - O #ignoreline
        # 'token': '7a88626f-77a8-4b13-bec6-554191c18c9e',  # CM - H - 1 #ignoreline
        'token': '86e6501b-59b7-4793-874c-bee84100e779',  # CM - O #ignoreline
        'ano': 2021,
        'entidade': 34 # idetificador 11968
    }
    mensagem_inicio(params_exec)
    interacao_cloud.verifica_token(params_exec['token'])
    verifica_tabelas_controle()
    # enviar(params_exec, 'campo-adicional') # Implementar    
    # enviar(params_exec, 'pais')
    # enviar(params_exec, 'estado')
    # enviar(params_exec, 'municipio')
    # enviar(params_exec, 'bairro')
    # enviar(params_exec, 'tipo-logradouro')
    # enviar(params_exec, 'logradouro')
    # enviar(params_exec, 'banco')
    # enviar(params_exec, 'agencia-bancaria')
    # enviar(params_exec, 'entidade') # Implementar
    # enviar(params_exec, 'feriado') # Implementar
    # enviar(params_exec, 'formacao') # Implementar
    # enviar(params_exec, 'fonte-divulgacao')
    # enviar(params_exec, 'natureza-texto-juridico')        
    # enviar(params_exec, 'motivo-alteracao-salarial')
    # enviar(params_exec, 'motivo-alteracao-cargo')
    # enviar(params_exec, 'tipo-movimentacao-pessoal')
    # enviar(params_exec, 'tipo-afastamento')
    # enviar(params_exec, 'motivo-rescisao')
    # enviar(params_exec, 'funcao-relogico') # Implementar
    # enviar(params_exec, 'tipo-cargo')
    # enviar(params_exec, 'tipo-ato')
    # enviar(params_exec, 'ato')
    # enviar(params_exec, 'plano-previdencia')
    # enviar(params_exec, 'cancelamento-ferias') # Implementar
    # enviar(params_exec, 'configuracao-ferias')    
    # enviar(params_exec, 'horario') # Implementar
    # enviar(params_exec, 'jornada-trabalho') # Implementar
    # enviar(params_exec, 'grupo-funcional') # Implementar
    # enviar(params_exec, 'configuracao-organograma')
    # enviar(params_exec, 'organograma')
    # enviar(params_exec, 'configuracao-lotacao-fisica')
    # enviar(params_exec, 'lotacao-fisica')
    # enviar(params_exec, 'formacao')
    # enviar(params_exec, 'pessoa-fisica')
    # enviar(params_exec, 'conta-bancaria')
    # enviar(params_exec, 'pessoa-juridica')
    # enviar(params_exec, 'dependencia')
    # enviar(params_exec, 'configuracao-encargos-sociais') # Implementar
    # enviar(params_exec, 'base')
    # enviar(params_exec, 'configuracao-evento')
    # enviar(params_exec, 'configura-media-vantagem') # Implementar
    # enviar(params_exec, 'plano-cargo-salario')
    # enviar(params_exec, 'nivel-salarial')
    # enviar(params_exec, 'classe-referencia') # Implementar    
    # enviar(params_exec, 'categoria-trabalhador')
    # enviar(params_exec, 'configuracao-adicional') # Implementar
    # enviar(params_exec, 'vinculo-empregaticio')
    # enviar(params_exec, 'cbo')    
    # enviar(params_exec, 'configuracao-licenca-premio') # Implementar
    # enviar(params_exec, 'cargo')
    # enviar(params_exec, 'concurso')                            
    # enviar(params_exec, 'matricula')
    # enviar(params_exec, 'servico-autonomo') # Implementar
    # enviar(params_exec, 'area-atuacao') # Implementar
    # enviar(params_exec, 'formacao') # Implementar
    # enviar(params_exec, 'profissional') # Implementar
    # enviar(params_exec, 'tipo-atestado') # Implementar
    # enviar(params_exec, 'motivo-consulta-medica') # Implementar
    # enviar(params_exec, 'atestado') # Implementar
    # enviar(params_exec, 'afastamento')
    # enviar(params_exec, 'transferencia') # Implementar
    # enviar(params_exec, 'lancamento-evento')
    # enviar(params_exec, 'rescisao')
    # enviar(params_exec, 'movimentacao-pessoal') # Implementar
    # enviar(params_exec, 'periodo-aquisitivo-ferias')
    # enviar(params_exec, 'periodo-aquisitivo-decimo-terceiro')
    # enviar(params_exec, 'calculo-folha-rescisao')
    # enviar(params_exec, 'calculo-folha-ferias')
    # enviar(params_exec, 'calculo-folha-decimo-terceiro')
    # enviar(params_exec, 'calculo-folha-mensal')
    enviar(params_exec, 'folha')
    # enviar(params_exec, 'configuracao-dirf') # Implementar
    # enviar(params_exec, 'mede-lotes')
    # winsound.PlaySound("SystemAsterisk", winsound.SND_ALIAS)


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
