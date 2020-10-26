import requests
import json
import re
import getpass
import settings
import math
import logging
from datetime import datetime


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
        print(f'Erro ao executar função "get_dados_token". {error}')


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
                retorno_requisicao.append(ret_envio)

                lotes_enviados += 1
                print(f'\r- Lotes enviados: {lotes_enviados}/{total_lotes}', end='')
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote(lote_envio, url=kwargs.get('url'),
                                    token=kwargs.get('token'), tipo_registro=kwargs.get('tipo_registro'))
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
        retorno_req = requests.post(url, headers=headers, data=json_envio_lote)
        # print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if 'json' in retorno_req.headers.get('Content-Type'):
            retorno_json = retorno_req.json()

            if 'id_lote' in retorno_json:
                retorno_requisicao['id_lote'] = retorno_json['idLote']
            elif 'id' in retorno_json:
                retorno_requisicao['id_lote'] = retorno_json['id']
            else:
                retorno_requisicao['id_lote'] = ''

            # print('DEBUG - Lote enviado: ', retorno_requisicao['id_lote'])

            if settings.SISTEMA_ORIGEM == 'folha':
                retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['id_lote']
            else:
                retorno_requisicao['url_consulta'] = re.sub('\w+$', f'lotes/{retorno_requisicao["idLote"]}', url)
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
    limit = 100
    offset = 0
    params = {'offset': 0, 'limit': limit}
    headers = {'authorization': f'bearer {params_exec["token"]}'}
    try:
        while has_next:
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url=url, params=params, headers=headers)
            retorno_json = r.json()
            has_next = retorno_json['hasNext']
            offset += limit
            if 'content' in retorno_json:
                for i in retorno_json['content']:
                    dados_coletados.append(i)


    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados