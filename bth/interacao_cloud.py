from datetime import datetime
import json
import re
import getpass
import math
import sys
import logging
import requests
import settings


def verifica_token(token):
    r = {"expired": True}
    print(f'\n:: Iniciando validação do token {token}')
    try:
        url = "https://oauth.cloud.betha.com.br/auth/oauth2/tokeninfo"
        params = {'access_token': token}
        req = requests.get(url=url, params=params)
        data = req.json()
        if 'error' in data.keys():
            r['error'] = data['error']
        if 'user' in data:
            dados_json = data['user']['attributes']['singleAccess']
            r['entityId'] = re.search("(?<=entityId\" : \")(\\d+)(?!=\")", str(dados_json)).group()
            r['databaseId'] = re.search("(?<=databaseId\" : \")(\\d+)(?!=\")", str(dados_json)).group()
        if 'expired' in data:
            r['expired'] = data['expired']
        if not r['expired']:
            print(f'- Token ativo. \n- Database: {r["databaseId"]} \n- Entidade: {r["entityId"]}')
        else:
            print('- Token inválido. Execução será finalizada.')
    except Exception as error:
        print(f'Erro ao executar função "verifica_token". {error}')


def get_dados_token(token):
    r = {}
    try:
        url = "https://oauth.cloud.betha.com.br/auth/oauth2/tokeninfo"
        params = {'access_token': token}
        req = requests.get(url=url, params=params)
        data = req.json()
        if 'user' in data:
            dados_json = data['user']['attributes']['singleAccess']
            r['entityId'] = re.search("(?<=entityId\" : \")(\\d+)(?!=\")", str(dados_json)).group()
            r['databaseId'] = re.search("(?<=databaseId\" : \")(\\d+)(?!=\")", str(dados_json)).group()
    except Exception as error:
        print(f'Erro ao executar função "get_dados_token". {error}')
    finally:
        return r


def preparar_requisicao_sem_lote(lista_dados, *args, **kwargs):
    # print('- Iniciando montagem e envio de lotes.')
    lista_retorno = []
    dh_inicio = datetime.now()
    lotes_enviados = 0
    total_lotes = len(lista_dados)
    total_erros = 0
    try:
        for i in lista_dados:
            lotes_enviados += 1
            # print(f'\r- Dados enviados: {lotes_enviados}/{total_lotes}', '\n' if lotes_enviados == total_lotes else '', end='')
            dict_envio = i
            hash_chaves = None
            if 'idIntegracao' in dict_envio:
                hash_chaves = dict_envio['idIntegracao']
                del dict_envio['idIntegracao']
            if 'url' in dict_envio:
                url = dict_envio['url']
                del dict_envio['url']
            else:
                url = kwargs.get('url')
            json_envio = json.dumps(dict_envio)
            retorno_requisicao = {
                'hash_chave': hash_chaves,
                'id_gerado': None,
                'mensagem': None
            }
            headers = {'authorization': f'bearer {kwargs.get("token")}', 'content-type': 'application/json'}
            # print('json', json_envio)
            retorno_req = requests.post(url, headers=headers, data=json_envio)
            # print('response', retorno_req.content)
            # print('retorno_req', retorno_req, retorno_req.text)
            if retorno_req.ok:
                retorno_requisicao['id_gerado'] = int(retorno_req.text)
                # lista_retorno.append(retorno_requisicao)
            else:
                retorno_json = retorno_req.json()
                if 'message' in retorno_json:
                    retorno_requisicao['mensagem'] = retorno_json['message']
                    # print('Erro: ', retorno_json['message'])
            if retorno_requisicao['id_gerado'] is None:
                total_erros += 1
            lista_retorno.append(retorno_requisicao)
        # print(f'- Envio finalizado. {total_erros} registro(s) retornaram inconsistência.')
    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')
    finally:
        return lista_retorno


def preparar_requisicao(lista_dados, *args, **kwargs):
    print('- Iniciando montagem e envio de lotes.')
    dh_inicio = datetime.now()
    retorno_requisicao = []
    lote_envio = []
    lotes_enviados = 0
    tamanho_lote = 1 if 'tamanho_lote' not in kwargs else kwargs.get('tamanho_lote')
    total_lotes = math.ceil(len(lista_dados) / tamanho_lote)
    try:
        for i in lista_dados:
            lote_envio.append(i)
            if len(lote_envio) >= tamanho_lote:
                ret_envio = enviar_lote(lote_envio,
                                        url=kwargs.get('url'),
                                        token=kwargs.get('token'),
                                        tipo_registro=kwargs.get('tipo_registro'))
                if ret_envio['id_lote'] is not None:
                    retorno_requisicao.append(ret_envio)
                lotes_enviados += 1
                print(f'\r- Lotes enviados: {lotes_enviados}/{total_lotes}', end='')
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote(lote_envio,
                                    url=kwargs.get('url'),
                                    token=kwargs.get('token'),
                                    tipo_registro=kwargs.get('tipo_registro'))
            if ret_envio['id_lote'] is not None:
                retorno_requisicao.append(ret_envio)
        if tamanho_lote != total_lotes:
            print(f'\r- Lotes enviados: {total_lotes}/{total_lotes}', end='')
        print(f'\n- Envio de lotes finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')
    finally:
        return retorno_requisicao


def enviar_lote(lote, *args, **kwargs):
    json_envio_lote = json.dumps(lote)
    retorno_requisicao = {
        'sistema': '300',
        'tipo_registro': kwargs.get('tipo_registro'),
        'data_hora_envio': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'usuario': f'(bthMigracao) {getpass.getuser()}',
        'url_consulta': None,
        'status': 1,
        'id_lote': None,
        'conteudo_json': json_envio_lote
    }
    try:
        url = kwargs.get('url')
        token = kwargs.get('token')
        headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
        # print("headers: ", headers)
        # print("url: ", url)
        # print("json_envio_lote: ", json_envio_lote)
        retorno_req = requests.post(url, headers=headers, data=json_envio_lote)
        # print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if retorno_req.ok:
            if 'json' in retorno_req.headers.get('Content-Type'):
                retorno_json = retorno_req.json()
                if 'id_lote' in retorno_json:
                    retorno_requisicao['id_lote'] = retorno_json['idLote']
                elif 'id' in retorno_json:
                    retorno_requisicao['id_lote'] = retorno_json['id']
                else:
                    print('DEBUG - retorno_json: ', retorno_json)
                    retorno_requisicao['id_lote'] = None
                print(':: Lote enviado: ', retorno_requisicao['id_lote'])
                if settings.SISTEMA_ORIGEM == 'folha':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['id_lote']
                else:
                    retorno_requisicao['url_consulta'] = re.sub('\\w+$', f'lotes/{retorno_requisicao["idLote"]}', url)
            else:
                print('Retorno não JSON:', retorno_req.status_code, retorno_req.text)
    except Exception as error:
        print(f'Erro durante a execução da função enviar_lote. {error}')
    finally:
        return retorno_requisicao


def busca_dados_cloud(params_exec, **kwargs):
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    limit = 20
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    headers = {'authorization': f'bearer {params_exec["token"]}'}
    try:
        while has_next:
            print(f'\r- Realizando busca na página {rodada_busca}', end='')
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url=url, params=params, headers=headers)

            if r.ok:
                retorno_json = r.json()
                # print('retorno_json', retorno_json)
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        dados_coletados.append(i)
                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)

            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False

        print('\n- Busca de páginas finalizada.')
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')
    finally:
        return dados_coletados


def busca_api_fonte_dados(params_exec, **kwargs):
    """
    Função para realizar a busca através das API's de fonte de dados Betha
    :param params_exec: Parâmetros de contexto da execução
    :param kwargs: Parâmetros
    :param campos: Listagem de campos que serão retornados da fonte
    :param criterio: Filtros que serão aplicados na busca da fonte
    :param ordenacao: Ordenação dos campos que serão retornados
    :return: Retorna um objeto <List> contendo os JSON's obtidos da fonte.
    """
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    campos = 'id' if 'campos' not in kwargs else kwargs.get('campos')
    criterio = None if 'criterio' not in kwargs else kwargs.get('criterio')
    ordenacao = None if 'ordenacao' not in kwargs else kwargs.get('ordenacao')
    limit = 100 if 'limit' not in kwargs else kwargs.get('limit')
    headers = {'authorization': f'bearer {params_exec["token"]}'}
    try:
        while has_next:
            params = {'offset': offset, 'limit': limit, 'fields': campos}
            if criterio is not None:
                params.update({'filter': criterio})
            if ordenacao is not None:
                params.update({'sort': ordenacao})
            r = requests.get(url=url, params=params, headers=headers)
            if r.ok:
                retorno_json = r.json()
                # print('retorno_json', retorno_json)
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        # print('DEBUG', i)
                        dados_coletados.append(i)
                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)
            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')
    finally:
        return dados_coletados
