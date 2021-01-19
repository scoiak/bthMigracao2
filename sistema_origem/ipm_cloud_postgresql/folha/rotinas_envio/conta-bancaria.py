from datetime import datetime
import re
import json
import logging
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud


sistema = 300
tipo_registro = 'conta-bancaria'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/pessoa-fisica'
limite_lote = 500


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if True:
        if params_exec.get('buscar') is True:
            busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print('- Busca de pessoas finalizada, iniciando verificação dos dados obtidos.')
    registros_formatados = []
    total_contas = 0
    try:
        for item in registros:
            if 'contasBancarias' in item and item['contasBancarias'] is not None:
                for item_conta in item['contasBancarias']:
                    hash_chaves = model.gerar_hash_chaves(sistema,
                                                          tipo_registro,
                                                          item['cpf'],
                                                          item_conta['numero'])
                    novo_registro = {
                        'sistema': sistema,
                        'tipo_registro': tipo_registro,
                        'hash_chave_dsk': hash_chaves,
                        'descricao_tipo_registro': 'Cadastro de Contas Bancarias de Pessoas Físicas',
                        'id_gerado': item_conta['id'],
                        'i_chave_dsk1': item['cpf'],
                        'i_chave_dsk2': item_conta['numero'],
                    }
                    registros_formatados.append(novo_registro)
                    total_contas += 1
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
        print(f'- Busca de {tipo_registro} finalizada. Foram executas {total_contas} contas. '
              f'Tabelas de controles atualizas com sucesso.')
    except Exception as error:
        print(f'Erro ao executar função "busca_dados_cloud". {error}')
