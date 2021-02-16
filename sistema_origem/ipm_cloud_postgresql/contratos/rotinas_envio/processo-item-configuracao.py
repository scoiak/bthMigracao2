import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'processo-item-configuracao'
url = 'https://compras.betha.cloud/compras/dados/api/processosadministrativositens'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # Busca dados do cloud
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    campos = 'configuracao(id), id, configuracao.processoAdministrativo.id'
    lista_dados = []
    criterio = f'configuracao.processoAdministrativo.parametroExercicio.exercicio = {params_exec["ano"]} and entidade.id = 56'

    registros_cloud = interacao_cloud.busca_api_fonte_dados(params_exec, url=url, campos=campos, criterio=criterio)

    if registros_cloud is not None and len(registros_cloud) > 0:
        for item in registros_cloud:
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['configuracao']['processoAdministrativo']['id'], item['id'])
            registro_encontrado = {
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro de Especificação de Material',
                'id_gerado': item['configuracao']['id'],
                'i_chave_dsk1': item['configuracao']['processoAdministrativo']['id'],
                'i_chave_dsk2': item['id']
            }
            # print('registro_encontrado', type(registro_encontrado), registro_encontrado)
            lista_dados.append(registro_encontrado)
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_dados)