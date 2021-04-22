import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'processo-participante-proposta-busca'
url = 'https://compras.betha.cloud/compras/dados/api/processosadministrativospropostasitens'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_base = coletar_dados(params_exec)

    # Busca dados do cloud
    busca_dados_cloud(params_exec, dados_base)


def busca_dados_cloud(params_exec, dados_base):
    print('- Iniciando busca de dados no cloud.')
    campos = 'id'
    contador = 1
    lista_dados = dados_base.to_dict('records')
    total_dados = len(lista_dados)

    for item in lista_dados:
        print(f'\r- Verificando registros: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        criterio = f'participante.fornecedor.pessoa.cpfCnpj = \'{item["cpf_participante"]}\' ' \
                   f'and item.id = {item["id_item"]} ' \
                   f'and processoAdministrativo.id = {item["id_processo"]}'
        registro_cloud = interacao_cloud.busca_api_fonte_dados(params_exec, url=url, campos=campos, criterio=criterio)

        if registro_cloud is not None:
            if len(registro_cloud) > 0:
                # print('id encontrado: ', registro_cloud[0]['id'], ' | hash: ', item["hash_chave_dsk"])
                logging.info(f'Atualizando registro \'{item["hash_chave_dsk"]}\' com o id {registro_cloud[0]["id"]}')
                model.atualiza_controle_migracao_registro(registro_cloud[0]['id'], item["hash_chave_dsk"])
        contador += 1


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