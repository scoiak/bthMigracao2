import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'comissao-membros'
url = 'https://compras.betha.cloud/compras/dados/api/comissoeslicitacaomembros'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # Busca dados do cloud
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    campos = 'id,responsavel(pessoa(cpfCnpj)),comissaoLicitacao(id),atribuicao'
    registro_cloud = interacao_cloud.busca_api_fonte_dados(params_exec, url=url, campos=campos)
    contador = 0
    dados = []

    for item in registro_cloud:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['comissaoLicitacao']['id'],
                                              item['responsavel']['pessoa']['cpfCnpj'], item['atribuicao'])
        registro_encontrado = {
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Especificação de Material',
            'id_gerado': item['id'],
            'i_chave_dsk1': item['comissaoLicitacao']['id'],
            'i_chave_dsk2': item['responsavel']['pessoa']['cpfCnpj'],
            'i_chave_dsk3': item['atribuicao']
        }
        # print('registro_encontrado', registro_encontrado)
        dados.append(registro_encontrado)
        contador += 1
    print(f'Busca de dados finalizada. Foram encotrados {contador} registros.')
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=dados)
    print(f'Tabelas de controle atualizadas com sucesso.')
