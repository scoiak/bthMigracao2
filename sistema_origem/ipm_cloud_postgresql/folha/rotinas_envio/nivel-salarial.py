import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
from datetime import datetime

sistema = 300
tipo_registro = 'nivel-salarial'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/nivel-salarial'
limite_lote = 500


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec)
    dados_enviar = pre_validar(params_exec, dados_assunto)
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')
    model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        dh_inicio = datetime.now()
        query = model.get_consulta(params_exec, f'{tipo_registro}.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- {len(df.index)} registro(s) encontrado(s).',
              f'\n- Consulta finalizada. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')
    finally:
        return df


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dh_inicio = datetime.now()
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Registros validados com sucesso: {len(dados_validados)} '
              f'| Registros com advertência: {len(registro_erros)}'
              f'\n- Pré-validação finalizada. ({(datetime.now() - dh_inicio).total_seconds()}) segundos')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando processo de transformação.')
    dh_inicio = datetime.now()
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    total_dados = len(dados)
    contador = 0
    for item in dados:
        contador += 1
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['codigo'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                "descricao": item['descricao'],
                "valor": item['valor'],
                "cargaHoraria": item['cargahoraria'],
                "coeficiente": item['coeficiente'],
                "inicioVigencia": item['iniciovigencia'],
                "dataHoraCriacao": item['datahoracriacao'],
                #"atoCriacao": item['atocriacao'],
                #"ultimoAto": item['ultimoato'],
                "planoCargoSalario": {
                    "id": item['planocargosalario']
                },
                "classesReferencias": item['classesreferencias'],
                "motivoAlteracao": item['motivoalteracao'],
                "reajusteSalarial": item['reajustesalarial']
            }
        }
        if 'atocriacao' in item and item['atocriacao'] is not None:
            dict_dados['conteudo'].update({
                'atoCriacao': {
                    'id': item['atocriacao']
                }
            })
        if 'ultimoato' in item and item['ultimoato'] is not None:
            dict_dados['conteudo'].update({
                'ultimoAto': {
                    'id': item['ultimoato']
                }
            })
        if item['historicos'] is not None:
            listahistorico = []
            totalhistorico = 0
            if len(listahistorico) > 1:
                lista = item['historicos'].split('%||%')
                for listacampo in lista:
                    totalhistorico += 1
                    campo = listacampo.split('%|%')
                    dict_item_historico = {
                        "descricao": campo[0],
                        "valor": campo[1],
                        "cargaHoraria": campo[2],
                        "coeficiente": campo[3],
                        "inicioVigencia": campo[4],
                        "dataHoraCriacao": campo[5],
                        "planoCargoSalario": {
                            "id": campo[8]
                        }
                    }
                    if campo[6] is not None:
                        dict_item_historico.update({
                            'atoCriacao': {
                                'id': campo[6]
                            }
                        })
                    if campo[7] is not None:
                        dict_item_historico.update({
                            'ultimoAto': {
                                'id': campo[7]
                            }
                        })
                    if campo[9] is not None:
                        dict_item_historico.update({
                            'classesReferencias': {
                                'id': campo[9]
                            }
                        })
                    if campo[10] is not None:
                        dict_item_historico.update({
                            'motivoAlteracao': {
                                'id': campo[10]
                            }
                        })
                    if campo[11] is not None:
                        dict_item_historico.update({
                            'reajusteSalarial': {
                                'id': campo[11]
                            }
                        })
                    if totalhistorico >= 1:
                        listahistorico.append(dict_item_historico)
            if len(listahistorico) > 0:
                dict_dados['conteudo'].update({
                    'historicos': listahistorico
                })
        print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Nível Salarial',
            'id_gerado': None,
            'i_chave_dsk1': item['codigo']
        })
    print(f'- Processo de transformação finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)
