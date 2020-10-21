import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # Verifica se existe algum lote pendente de execução
    # model.valida_lotes_enviados(params_exec)

    # Realiza rotina de busca dos dados no cloud
    busca_dados(params_exec)


def busca_dados(params_exec):
    print('- Iniciando busca de dados no cloud.')
    url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/pais'
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []

    for item in registros:
        hash_chaves = model.gerar_hash_chaves('300', 'paises', item['nome'])
        registros_formatados.append({
        'sistema': 300,
        'tipo_registro': 'paises',
        'hash_chave_dsk': hash_chaves,
        'descricao_tipo_registro': 'Cadastro de Paises',
        'id_gerado': item['id'],
        'i_chave_dsk1': item['nome']
        })
    model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=registros_formatados)
    print('- Busca de paises finalizada. Tabelas de controles atualizas com sucesso.')