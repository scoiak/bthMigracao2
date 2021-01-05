import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'logradouro'
url = 'https://compras.betha.cloud/compras-services/api/logradouros'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # Busca os dados cadastrados no cloud
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    try:
        for item in registros:
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['nome'], item['municipio']['id'])
            registros_formatados.append({
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro de Logradouros',
                'id_gerado': item['id'],
                'i_chave_dsk1': item['nome'],
                'i_chave_dsk2': item['municipio']['id']
            })
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
        print(f'- Busca de {tipo_registro} finalizada. Tabelas de controles atualizas com sucesso.')
    except Exception as error:
        print(f'Erro ao executar função "busca_dados_cloud". {error}')