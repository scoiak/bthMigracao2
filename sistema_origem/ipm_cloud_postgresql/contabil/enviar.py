"""
    ROTINA PRINCIPAL QUE É CHAMADA ARA O ENVIO
"""
import settings
import bth.interacao_cloud as interacao_cloud


def iniciar():
    params_exec = {
        'somente_pre_validar': False,
        'token': '533404da-8e5b-4d9f-b876-c9891d188d3b',
        'ano': '2020',
        'clicode': '00601'
    }
    # Exibe mensagem inicial de início de execução
    mensagem_inicio(params_exec)

    # Realiza a validação do token informado
    interacao_cloud.verifica_token(params_exec['token'])

    # Inicia chamadas de rotinas de envio de dados

    # enviar(params_exec, 'paises')
    # enviar(params_exec, 'estados')
    # enviar(params_exec, 'cidades')
    # enviar(params_exec, 'tiposLogradouros')
    # enviar(params_exec, 'logradouros')
    # enviar(params_exec, 'distritos')
    # enviar(params_exec, 'bairros')
    # enviar(params_exec, 'loteamentos')
    # enviar(params_exec, 'condominios')
    # enviar(params_exec, 'naturezaJuridica')
    # enviar(params_exec, 'tiposAdministracao')
    # enviar(params_exec, 'naturezaTextoJuridico')
    # enviar(params_exec, 'fontesDivulgacoes')
    # enviar(params_exec, 'tiposAtos')
    # enviar(params_exec, 'atos')
    # enviar(params_exec, '')
    # enviar(params_exec, 'configuracoesFuncionais')
    # enviar(params_exec, 'configuracoesNaturezaDespesas')
    # enviar(params_exec, 'configuracaoNaturezasReceitas')
    # enviar(params_exec, 'configuracoesOrganogramas')
    # enviar(params_exec, 'configuracoesPlanoContas')
    # enviar(params_exec, 'configuracoesRecursos')
    # enviar(params_exec, 'parametrosOrcamentarios')
    # enviar(params_exec, 'parametrosEscrituracao')
    # enviar(params_exec, 'ppas')
    # enviar(params_exec, 'ldos')
    # enviar(params_exec, 'loas')
    # enviar(params_exec, 'organogramas')
    # enviar(params_exec, 'naturezaDespesas')
    # enviar(params_exec, 'naturezaReceitas')
    # enviar(params_exec, 'recursos')
    # enviar(params_exec, 'funcoes')
    # enviar(params_exec, 'subFuncoes')
    # enviar(params_exec, 'programas')
    # enviar(params_exec, 'acoes')
    # enviar(params_exec, 'deducaoReceita')
    # enviar(params_exec, 'localizadores')
    # enviar(params_exec, 'produtos')
    # enviar(params_exec, 'unidadesDeMedida')
    # enviar(params_exec, 'receitas')
    # enviar(params_exec, 'gruposDespesasLoa')
    # enviar(params_exec, 'despesas')
    # enviar(params_exec, 'receitasLdo')
    # enviar(params_exec, 'metasFiscaisReceitas')
    # enviar(params_exec, 'gruposDespesasLdo')
    # enviar(params_exec, 'despesasLdo')
    # enviar(params_exec, 'metasFiscaisDespesas')
    # enviar(params_exec, 'tiposCompensacoes')
    # enviar(params_exec, 'tiposRenunciasFiscasi')
    # enviar(params_exec, 'renunciasFiscais')
    # enviar(params_exec, 'expansoesDespesas')
    # enviar(params_exec, 'tiposResultadosNominais')
    # enviar(params_exec, 'resultadosNominais')
    # enviar(params_exec, 'tiposRiscosFiscais')
    # enviar(params_exec, 'riscosFiscais')
    # enviar(params_exec, 'atuario')
    # enviar(params_exec, 'projecoesAtuarias')
    # enviar(params_exec, 'receitasPpa')
    # enviar(params_exec, 'gruposDespesasPpa')
    # enviar(params_exec, 'despesasPpa')
    # enviar(params_exec, 'tiposResponsaveis')
    # enviar(params_exec, 'responsaveis')
    # enviar(params_exec, 'equipesPlanejamento')
    # enviar(params_exec, 'orientacoesEstrategicasGoverno')
    # enviar(params_exec, 'audiencias')
    # enviar(params_exec, 'sugestoes')
    enviar(params_exec, 'credores')



def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do assunto {tipo_registro}')
    path_padrao = 'sistema_origem.ipm_cloud_postgresql.contabil.rotinas_envio'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)


def mensagem_inicio(params_exec):
    print(f':: Iniciando execução da migração do sistema {settings.BASE_ORIGEM} para Betha Cloud utilicando os '
          f'seguintes parâmetros: \n- {params_exec}')
