import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import re
import requests
import logging
from datetime import datetime

sistema = 300
tipo_registro = 'mede-lotes'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/natureza-texto-juridico'
limite_lote = 500


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # dados_assunto = coletar_dados(params_exec)
    # dados_enviar = pre_validar(params_exec, dados_assunto)
    if not params_exec.get('somente_pre_validar'):
        pass
        # iniciar_envio(params_exec, dados_enviar, 'POST')

    ler_lotes()


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


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Pré-validação finalizada. Registros validados com sucesso: '
              f'{len(dados_validados)} | Registros com advertência: {len(registro_erros)}')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando verificação dos lotes.')
    hoje = datetime.now().strftime("%Y-%m-%d")
    for item in dados:
        headers = {'authorization': f'bearer {params_exec["token"]}'}
        r = requests.get(url=item['url_consulta'], headers=headers)
        if r.ok:
            retorno_json = r.json()

            if re.search('\.', retorno_json['createdIn']):
                dia_envio = retorno_json['createdIn']
                dia_envio = datetime.strptime(dia_envio, '%Y-%m-%dT%H:%M:%S.%f').strftime("%d-%m-%Y")
                dt_envio = retorno_json['createdIn']
                dt_envio = datetime.strptime(dt_envio, '%Y-%m-%dT%H:%M:%S.%f')
            else:
                dia_envio = retorno_json['createdIn']
                dia_envio = datetime.strptime(dia_envio, '%Y-%m-%dT%H:%M:%S').strftime("%d-%m-%Y")
                dt_envio = retorno_json['createdIn']
                dt_envio = datetime.strptime(dt_envio, '%Y-%m-%dT%H:%M:%S')

            if re.search('\.', retorno_json['updatedIn']):
                dt_retorno = retorno_json['updatedIn']
                dt_retorno = datetime.strptime(dt_retorno, '%Y-%m-%dT%H:%M:%S.%f')
            else:
                dt_retorno = retorno_json['updatedIn']
                dt_retorno = datetime.strptime(dt_retorno, '%Y-%m-%dT%H:%M:%S')

            logging.info(f'{item["url_consulta"]};{dia_envio};{(dt_retorno - dt_envio)}')
            erros_consecutivos = 0
        else:
            erros_consecutivos += 1
            print('\nErro ao realizar requisição.', r.status_code)


def ler_lotes():
    print('Iniciando leitura de lotes')
    headers = {'authorization': f'bearer 144e13ad-29ce-49b7-b9dc-7d95ee29b0f6'}
    f = open("lista_lotes.txt", "r")
    for x in f:
        url = x.replace('\n', ' ').replace('\r', '').replace(' ', '')
        # print('url', url)
        r = requests.get(url=url, headers=headers)
        # print(r.status_code, r.content)
        if r.ok:
            retorno_json = r.json()

            if re.search('\.', retorno_json['createdIn']):
                dia_envio = retorno_json['createdIn']
                dia_envio = datetime.strptime(dia_envio, '%Y-%m-%dT%H:%M:%S.%f').strftime("%d-%m-%Y")
                dt_envio = retorno_json['createdIn']
                dt_envio = datetime.strptime(dt_envio, '%Y-%m-%dT%H:%M:%S.%f')
            else:
                dia_envio = retorno_json['createdIn']
                dia_envio = datetime.strptime(dia_envio, '%Y-%m-%dT%H:%M:%S').strftime("%d-%m-%Y")
                dt_envio = retorno_json['createdIn']
                dt_envio = datetime.strptime(dt_envio, '%Y-%m-%dT%H:%M:%S')

            if re.search('\.', retorno_json['updatedIn']):
                dt_retorno = retorno_json['updatedIn']
                dt_retorno = datetime.strptime(dt_retorno, '%Y-%m-%dT%H:%M:%S.%f')
            else:
                dt_retorno = retorno_json['updatedIn']
                dt_retorno = datetime.strptime(dt_retorno, '%Y-%m-%dT%H:%M:%S')

            duracao = int((dt_retorno - dt_envio).total_seconds())
            # logging.info(f'{url};{dia_envio};{(dt_retorno - dt_envio)}')
            if duracao > 90:
                logging.info(f';{url};{dia_envio};{dt_envio};{dt_retorno};{duracao}')
                print(url, dia_envio, dt_envio, dt_retorno)