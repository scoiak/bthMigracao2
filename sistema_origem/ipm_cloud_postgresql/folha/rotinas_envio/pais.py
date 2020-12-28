from datetime import datetime
import re
import json
import logging
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

tipo_registro = 'pais'
sistema = 300
limite_lote = 500
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/pais'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    busca_dados(params_exec)


def busca_dados(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    for item in registros:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['nome'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Pais',
            'id_gerado': item['id'],
            'i_chave_dsk1': item['nome']
        })
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
    print('- Busca de paises finalizada. Tabelas de controles atualizas com sucesso.')
