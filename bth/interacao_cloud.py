import requests
import json
import re
import getpass
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
    retorno_requisicao = []
    lote_envio = []
    tamanho_lote = 1 if 'tamanho_lote' not in kwargs else kwargs.get('tamanho_lote')
    try:
        for i in lista_dados:
            lote_envio.append(i)
            if len(lote_envio) >= tamanho_lote:
                ret_envio = enviar_lote(lote_envio, url=kwargs.get('url'),
                                        token=kwargs.get('token'), tipo_registro=kwargs.get('tipo_registro'))
                retorno_requisicao.append(ret_envio)
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote(lote_envio, url=kwargs.get('url'),
                                    token=kwargs.get('token'), tipo_registro=kwargs.get('tipo_registro'))
            retorno_requisicao.append(ret_envio)

    except Exception as error:
        print(f'Erro durante a execução da função enviar_requisicao. {error}')

    finally:
        return retorno_requisicao


def enviar_lote(lote, *args, **kwargs):
    json_envio_lote = json.dumps(lote)
    retorno_requisicao = {
        'sistema': '1',
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
        if 'json' in retorno_req.headers.get('Content-Type'):
            retorno_json = retorno_req.json()
            retorno_requisicao['id_lote'] = retorno_json['idLote']
            retorno_requisicao['url_consulta'] = re.sub('\w+$', f'lotes/{retorno_json["idLote"]}', url)
        else:
            print('Retorno não JSON:', retorno_req.status_code, retorno_req.text)

    except Exception as error:
        print(f'Erro durante a execução da função enviar_requisicao. {error}')

    finally:
        return retorno_requisicao
