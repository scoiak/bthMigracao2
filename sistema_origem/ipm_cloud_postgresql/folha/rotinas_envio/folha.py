from datetime import datetime
import re
import json
import logging
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

sistema = 300
tipo_registro = 'folha'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/folha'
limite_lote = 100


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if False:
        if params_exec.get('buscar') is True:
            busca_dados(params_exec)
    if True:
        if params_exec.get('enviar') is True:
            dados_assunto = coletar_dados(params_exec)
            dados_enviar = pre_validar(params_exec, dados_assunto)
            if not params_exec.get('somente_pre_validar'):
                iniciar_envio(params_exec, dados_enviar, 'POST')
    model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def busca_dados(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    for item in registros:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, params_exec.get('entidade'), item['matricula']['id'], item['tipoProcessamento'], item['subTipoProcessamento'], item['competencia'], item['dataPagamento'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro da Folha',
            'id_gerado': item['id'],
            'i_chave_dsk1': params_exec.get('entidade'),
            'i_chave_dsk2': item['matricula']['id'],
            'i_chave_dsk3': item['tipoProcessamento'],
            'i_chave_dsk4': item['subTipoProcessamento'],
            'i_chave_dsk5': item['competencia'],
            'i_chave_dsk6': item['dataPagamento']
        })
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
    print('- Busca finalizada. Tabelas de controles atualizas com sucesso.')


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
              f'\n- Pré-validação finalizada. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
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
        # print(f'\r- Gerando JSON: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['entidade'], item['matricula'], item['tipoprocessamento'], item['subtipoprocessamento'], item['competencia'], item['datapagamento'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'tipoProcessamento': item['tipoprocessamento'],
                'subTipoProcessamento': item['subtipoprocessamento'],
                'matricula': {
                    'id': item['matricula']
                },
                'calculo': {
                    'id': item['calculo']
                }
            }
        }
        if 'competencia' in item and item['competencia'] is not None:
            dict_dados['conteudo'].update({'competencia': item['competencia']})
        if 'folhapagamento' in item and item['folhapagamento'] is not None:
            dict_dados['conteudo'].update({'folhaPagamento': item['folhapagamento']})
        if 'totalbruto' in item and item['totalbruto'] is not None:
            dict_dados['conteudo'].update({'totalBruto': item['totalbruto']})
        if 'totaldesconto' in item and item['totaldesconto'] is not None:
            dict_dados['conteudo'].update({'totalDesconto': item['totaldesconto']})
        if 'totalliquido' in item and item['totalliquido'] is not None:
            dict_dados['conteudo'].update({'totalLiquido': item['totalliquido']})
        if 'datafechamento' in item and item['datafechamento'] is not None:
            dict_dados['conteudo'].update({'dataFechamento': item['datafechamento']})
        if 'datapagamento' in item and item['datapagamento'] is not None:
            dict_dados['conteudo'].update({'dataPagamento': item['datapagamento']})
        if 'dataliberacao' in item and item['dataliberacao'] is not None:
            dict_dados['conteudo'].update({'dataLiberacao': item['dataliberacao']})
        if 'datacalculo' in item and item['datacalculo'] is not None:
            dict_dados['conteudo'].update({'dataCalculo': item['datacalculo']})
        if 'situacao' in item and item['situacao'] is not None:
            dict_dados['conteudo'].update({'situacao': item['situacao']})
        if 'conversao' in item and item['conversao'] is not None:
            dict_dados['conversao'].update({'conversao': item['conversao']})
        if item['eventos'] is not None:
            listaconteudo = []
            lista = item['eventos'].split('%||%')
            if len(lista) > 0:
                for listacampo in lista:
                    campo = listacampo.split('%|%')
                    for idx, val in enumerate(campo):
                        if campo[idx] == '':
                            campo[idx] = None
                    dict_item_conteudo = {
                        'configuracao': {
                            'id': campo[0]
                        },
                        'tipo': campo[1],
                        'referencia': campo[2],
                        'valor': campo[3],
                    }
                    if campo[4] is not None:
                        dict_item_conteudo.update({
                            'periodosAquisitivosFerias': campo[4]
                        })
                    if campo[5] is not None:
                        campolistaconteudo = []
                        campolista = campo[5].split('%&&%')
                        if len(lista) > 0:
                            for campolistacampo in campolista:
                                campocampo = campolistacampo.split('%&%')
                                for idx, val in enumerate(campocampo):
                                    if campocampo[idx] == '':
                                        campocampo[idx] = None
                                campodict_item_conteudo = {
                                    'dependencia': {
                                        'id': int(campocampo[0])
                                    }
                                }
                                if campocampo[1] is not None:
                                    campodict_item_conteudo.update({
                                        'valor': campocampo[1]
                                    })
                                campolistaconteudo.append(campodict_item_conteudo)
                        if len(campolistaconteudo) > 0:
                            dict_item_conteudo.update({
                                'rateioDependentes': campolistaconteudo
                            })
                    listaconteudo.append(dict_item_conteudo)
            if len(listaconteudo) > 0:
                dict_dados['conteudo'].update({
                    'eventos': listaconteudo
                })
        if item['composicaobases'] is not None:
            listaconteudo = []
            lista = item['composicaobases'].split('%||%')
            if len(lista) > 0:
                for listacampo in lista:
                    campo = listacampo.split('%|%')
                    for idx, val in enumerate(campo):
                        if campo[idx] == '':
                            campo[idx] = None
                    dict_item_conteudo = {
                        'configuracaoEvento': {
                            'id': campo[0]
                        },
                        'base': {
                            'id': campo[1]
                        },
                        'valor': campo[2],
                    }
                    listaconteudo.append(dict_item_conteudo)
            if len(listaconteudo) > 0:
                dict_dados['conteudo'].update({
                    'composicaoBases': listaconteudo
                })
        if params_exec.get('atualizar') is True:
            if item['idcloud'] is not None:
                dict_dados['conteudo'].update({
                    'id': int(item['idcloud'])
                })
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro da Folha',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['entidade'],
            'i_chave_dsk2': item['matricula'],
            'i_chave_dsk3': item['tipoprocessamento'],
            'i_chave_dsk4': item['subtipoprocessamento'],
            'i_chave_dsk5': item['competencia'],
            'i_chave_dsk6': item['datapagamento']
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
