import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'processo-entidade-item'
url = 'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoAdministrativoId}/itens/{itemId}/entidades'

"""
Foi desenvolvido um script chamado 'ks - limpa divisao itens por entidade do processo' para realizar a limpeza
dos processos que possuem mais de um entidade participante. Deve-se executar esse script antes dessa rotina.
A rotina também está configurada para realizar o envio das divisões apenas de processos com mais de uma entidade.
"""

def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # T - Realiza a pré-validação dos dados
    dados_enviar = pre_validar(params_exec, dados_assunto)

    # L - Realiza o envio dos dados validados
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        tempo_inicio = datetime.now()
        query = model.get_consulta(params_exec, f'{tipo_registro}.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def list_unique(lista):
    list_of_unique = []
    for item in lista:
        if item not in list_of_unique:
            list_of_unique.append(item)
    return list_of_unique


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True

            # INSERIR AS REGRAS DE PRÉ VALIDAÇÃO AQUI

            if registro_valido:
                dados_validados.append(linha)

        print(f'- Pré-validação finalizada. Registros validados com sucesso: '
              f'{len(dados_validados)} | Registros com advertência: {len(registro_erros)}')

    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')

    finally:
        return dados_validados


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando envio dos dados.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    total_dados = len(dados)
    contador = 0
    total_erros = 0

    for item in dados:
        lista_dados_enviar = []
        lista_controle_migracao = []
        contador += 1
        print(f'\r- Enviando registros: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['clicodigomin'], item['ano_processo'],
                                              item['nro_processo'], item['separador'],
                                              item['nro_lote'], item['separador'], item['numero_item_original'],
                                              item['separador'], item['clicodigoreq'])
        url_parametrizada = url.replace('{exercicio}', str(item['ano_processo']))\
                               .replace('{processoAdministrativoId}', str(item['id_processo']))\
                               .replace('{itemId}', str(item['id_item']))
        dict_dados = {
            'idIntegracao': hash_chaves,
            'url': url_parametrizada,
            'item': {
                'id': item['id_item']
            },
            'entidadeParticipante': {
                'id': item['id_entidade_participante']
            },
            'quantidadeDistribuida': item['qtd_final']
        }

        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Divisão de itens por entidade do Processo',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['clicodigomin'],
            'i_chave_dsk2': item['ano_processo'],
            'i_chave_dsk3': item['nro_processo'],
            'i_chave_dsk4': item['separador'],
            'i_chave_dsk5': item['nro_lote'],
            'i_chave_dsk6': item['separador'],
            'i_chave_dsk7': item['numero_item_original'],
            'i_chave_dsk8': item['separador'],
            'i_chave_dsk9': item['clicodigoreq']
        })

        if True:
            model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
            req_res = interacao_cloud\
                .preparar_requisicao_sem_lote(
                    lista_dados=lista_dados_enviar,
                    token=token,
                    url=url,
                    tipo_registro=tipo_registro)
            model.atualiza_tabelas_controle_envio_sem_lote(params_exec, req_res, tipo_registro=tipo_registro)
            if req_res[0]['mensagem'] is not None:
                total_erros += 1
                # break
    if total_erros > 0:
        print(f'- Envio finalizado. Foram encontrados um total de {total_erros} inconsistência(s) de envio.')
    else:
        print('- Envio de dados finalizado sem inconsistências.')


