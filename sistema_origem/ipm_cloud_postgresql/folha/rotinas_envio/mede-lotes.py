from datetime import datetime
import re
import json
import logging
import pandas as pd
import requests
import matplotlib.pyplot as plt
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud


sistema = 300
tipo_registro = 'mede-lotes'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_lotes = coletar_dados(params_exec)
    ler_lotes(params_exec, dados_lotes)


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
        print(f'Erro ao executar função "coletar_dados". {error}')
    finally:
        return df


def ler_lotes(params_exec, dados_lotes):
    contador = 0
    total_dados = len(dados_lotes)
    headers = {'authorization': f'bearer {params_exec["token"]}'}
    dados_coletados = []
    dados_validados = []
    lista_dados = dados_lotes.to_dict('records')
    for linha in lista_dados:
        registro_valido = True
        if registro_valido:
            dados_validados.append(linha)
    for item in dados_validados:
        # print('item', type(item), item)
        contador += 1
        print(f'\r- Verificando lote: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        r = requests.get(url=item['url_consulta'], headers=headers)
        if r.ok:
            retorno_json = r.json()
            if re.search('r\.', retorno_json['createdIn']):
                dia_envio = retorno_json['createdIn']
                dia_envio = datetime.strptime(dia_envio, '%Y-%m-%dT%H:%M:%S.%f').strftime("%d-%m-%Y")
                dt_envio = retorno_json['createdIn']
                dt_envio = datetime.strptime(dt_envio, '%Y-%m-%dT%H:%M:%S.%f')
                hr_envio = datetime.strptime(retorno_json['createdIn'], '%Y-%m-%dT%H:%M:%S.%f').hour
            else:
                dia_envio = retorno_json['createdIn']
                dia_envio = datetime.strptime(dia_envio, '%Y-%m-%dT%H:%M:%S').strftime("%d-%m-%Y")
                dt_envio = retorno_json['createdIn']
                dt_envio = datetime.strptime(dt_envio, '%Y-%m-%dT%H:%M:%S')
                hr_envio = datetime.strptime(retorno_json['createdIn'], '%Y-%m-%dT%H:%M:%S').hour
            if re.search('r\.', retorno_json['updatedIn']):
                dt_retorno = retorno_json['updatedIn']
                dt_retorno = datetime.strptime(dt_retorno, '%Y-%m-%dT%H:%M:%S.%f')
            else:
                dt_retorno = retorno_json['updatedIn']
                dt_retorno = datetime.strptime(dt_retorno, '%Y-%m-%dT%H:%M:%S')
            duracao = int((dt_retorno - dt_envio).total_seconds())
            if duracao > 1:
                logging.info(f';{item["url_consulta"]};{dia_envio};{dt_envio};{dt_retorno};{duracao}')
                # print(item['url_consulta'], dia_envio, dt_envio, dt_retorno, hr_envio, duracao)
                hora = ("0" + str(hr_envio))[-2:] + ":00"
                dados_coletados.append([hora, duracao])
    trabalha_resultado(dados_coletados)


def trabalha_resultado(dados_coletados):
    print('- Iniciando trabalho com o resultado')
    df = pd.DataFrame(dados_coletados, columns=['hr_envio', 'duracao'])
    for i in range(24):
        if i not in df.hr_envio.values:
            hora = ("0" + str(i))[-2:] + ":00"
            df = df.append({'hr_envio': hora, 'duracao': 0}, ignore_index=True)
    df_group = df.groupby(['hr_envio']).mean()
    df_group.plot.bar()
    plt.grid(True, linestyle='--', which='major', color='grey', alpha=.25)
    plt.axvline(50, color='red', alpha=0.25)
    plt.show()
