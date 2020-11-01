import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import re
import json
import logging
from datetime import datetime

sistema = 300
tipo_registro = 'banco'
limite_lote = 500
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/banco'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    try:
        for item in registros:
            codigotexto = str.replace(item['numeroBanco'], '-', '')
            if not re.search("[a-zA-Z]", codigotexto):
                codigo = str(int(codigotexto))
                hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, codigo)
                registros_formatados.append({
                    'sistema': sistema,
                    'tipo_registro': tipo_registro,
                    'hash_chave_dsk': hash_chaves,
                    'descricao_tipo_registro': 'Cadastro de Banco',
                    'id_gerado': item['id'],
                    'i_chave_dsk1': codigo
                })
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
        print(f'- Busca de {tipo_registro} finalizada. Tabelas de controles atualizas com sucesso.')
    except Exception as error:
        print(f'Erro ao executar função "busca_dados_cloud". {error}')
