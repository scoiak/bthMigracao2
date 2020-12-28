from datetime import datetime
import re
import json
import logging
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

sistema = 300
tipo_registro = 'rescisao'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/rescisao'
limite_lote = 1000


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if False:
        busca_dados(params_exec)
    if True:
        dados_assunto = coletar_dados(params_exec)
        dados_enviar = pre_validar(params_exec, dados_assunto)
        if not params_exec.get('somente_pre_validar'):
            iniciar_envio(params_exec, dados_enviar, 'POST')
        model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def busca_dados(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    for item in registros:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, '56', item['matricula']['id'], item['data'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Bairro',
            'id_gerado': item['id'],
            'i_chave_dsk1': '56',
            'i_chave_dsk2': item['matricula']['id'],
            'i_chave_dsk3': item['data']
        })
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
    print('- Busca finalizada. Tabelas de controles atualizas com sucesso.')


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        dh_inicio = datetime.now()
        query = model.get_consulta(params_exec, f'{tipo_registro}.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- {len(df.index)} registro(s) encontrado(s).',
              f'\n- Consulta finalizada. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')
    finally:
        return df


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dh_inicio = datetime.now()
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Registros validados com sucesso: {len(dados_validados)} '
              f'| Registros com advertência: {len(registro_erros)}'
              f'\n- Pré-validação finalizada. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando processo de transformação.')
    dh_inicio = datetime.now()
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    total_dados = len(dados)
    contador = 0
    for item in dados:
        contador += 1
        # print(f'\r- Gerando JSON: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['entidade'], item['matricula'], item['data'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'data': item['data'],
                'matricula': {
                    'id': item['matricula']
                },
                'motivoRescisao': {
                    'id': item['motivorescisao']
                }
            }
        }
        if 'conversao' in item and item['conversao'] is not None:
            dict_dados['conteudo'].update({'conversao': item['conversao']})
        if 'saldofgts' in item and item['saldofgts'] is not None:
            dict_dados['conteudo'].update({'saldoFgts': item['saldofgts']})
        if 'fgtsmesanterior' in item and item['fgtsmesanterior'] is not None:
            dict_dados['conteudo'].update({'fgtsMesAnterior': item['fgtsmesanterior']})
        if 'avisoprevio' in item and item['avisoprevio'] is not None:
            dict_dados['conteudo'].update({'avisoPrevio': item['avisoprevio']})
        if 'ato' in item and item['ato'] is not None:
            dict_dados['conteudo'].update({
                'ato': {
                    'id': int(item['ato'])
                }})
        print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Rescisao',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['entidade'],
            'i_chave_dsk2': item['matricula'],
            'i_chave_dsk3': item['data']
        })
    print(f'- Processo de transformação finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)
