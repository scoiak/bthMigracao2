import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'material-especificacao'
url = 'https://compras.betha.cloud/compras/dados/api/materiaisespecificacoes'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_base = coletar_dados(params_exec)

    # Busca dados do cloud
    busca_dados_cloud(params_exec, dados_base)


def busca_dados_cloud(params_exec, dados_base):
    print('- Iniciando busca de dados no cloud.')
    campos = 'id, material.id'
    contador = 1
    lista_dados = dados_base.to_dict('records')
    total_dados = len(lista_dados)

    for item in lista_dados:
        print(f'\r- Enviando registros: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        criterio = f'material.id = {item["id_material"]}'
        registro_cloud = interacao_cloud.busca_api_fonte_dados(params_exec, url=url, campos=campos, criterio=criterio)

        if registro_cloud is not None:
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item["codigo_produto"])
            registro_encontrado = {
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro de Especificação de Material',
                'id_gerado': registro_cloud[0]['id'],
                'i_chave_dsk1': item["codigo_produto"]
            }
            model.insere_tabela_controle_migracao_registro(params_exec, lista_req=[registro_encontrado])
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