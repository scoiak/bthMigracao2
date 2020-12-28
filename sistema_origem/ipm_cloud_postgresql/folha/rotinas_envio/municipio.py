from datetime import datetime
import re
import json
import logging
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

sistema = 300
tipo_registro = 'municipio'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/municipio'
limite_lote = 500

def iniciar_processo_envio(params_exec, *args, **kwargs):
    if True:
        busca_dados_cloud(params_exec)
    if True:
        dados_assunto = coletar_dados(params_exec)
        dados_enviar = pre_validar(params_exec, dados_assunto)
        if not params_exec.get('somente_pre_validar'):
            iniciar_envio(params_exec, dados_enviar, 'POST')
        model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)

def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    try:
        for item in registros:
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['nome'], item['estado']['id'])
            registros_formatados.append({
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro de Município',
                'id_gerado': item['id'],
                'i_chave_dsk1': item['nome'],
                'i_chave_dsk2': item['estado']['id']
            })
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
        print('- Busca de paises finalizada. Tabelas de controles atualizas com sucesso.')
    except Exception as error:
        print(f'Erro ao executar função "busca_dados_cloud". {error}')

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

def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Pré-validação finalizada. Registros validados com sucesso: '
              f'{len(dados_validados)} | Registros com advertência: {len(registro_erros)}')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados

def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando envio dos dados.')
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    contador = 0
    for item in dados:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['nome'], item['estado'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                "nome": item['nome'],
                "estado": {
                    "id": item['estado']
                }
            }
        }
        if item['cep'] is not None:
            dict_dados['conteudo'].update({
                'cep': item['cep']
            })
        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Municipio',
            'id_gerado': None,
            'i_chave_dsk1': item['nome'],
            'i_chave_dsk2': item['estado']
        })
    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)
        print('- Envio de dados finalizado.')