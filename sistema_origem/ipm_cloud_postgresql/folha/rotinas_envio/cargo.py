from datetime import datetime
import re
import json
import logging
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

sistema = 300
tipo_registro = 'cargo'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/cargo'
limite_lote = 500


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if False: # Sem Funcionamento
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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, params_exec.get('entidade'), item['sigla'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Base',
            'id_gerado': item['id'],
            'i_chave_dsk1': params_exec.get('entidade'),
            'i_chave_dsk2': item['sigla']
        })
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
    print('- Busca finalizada. Tabelas de controles atualizas com sucesso.')


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


def list_unique(lista):
    list_of_unique = []
    for item in lista:
        if item not in list_of_unique:
            list_of_unique.append(item)
    return list_of_unique


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
    total_dados = len(dados)
    contador = 0
    for item in dados:
        contador += 1
        print(f'\r- Gerando JSON: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['entidade'], item['codigo'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'descricao': item['descricao'],
                'acumuloCargos': item['acumulocargos'],
                'quantidadeVagas': item['quantidadevagas'],
                'dedicacaoExclusiva': item['dedicacaoexclusiva'],
                'inicioVigencia': item['iniciovigencia'].strftime("%Y-%m-%d 02:%M:%S"),
                'pagaDecimoTerceiroSalario': item['pagadecimoterceirosalario'],
                'contagemEspecial': item['contagemespecial'],
                'quantidadeVagasPcd': item['quantidadevagaspcd'],
                'extinto': item['extinto'],           
                'tipo': {
                    'id': item['tipocargo']
                }                
             }
        }
        if 'cbo' in item and item['cbo'] is not None:
            dict_dados['conteudo'].update({
                'cbo': {
                    'id': item['cbo']
                }
            })
        if 'ato' in item and item['ato'] is not None:
            dict_dados['conteudo'].update({
                'ato': {
                    'id': item['ato']
                }
            })
        if 'camposadicionais' in item and item['camposadicionais'] is not None:
            dict_dados['conteudo'].update({
                'camposAdicionais': {
                    'tipo': 'CARGO',
                    'campos': [
                        {
                            'id': '5fafc50a001f7a0104272606',
                            'valor': item['tcetipoquadro']
                        },
                        {
                            'id': '5fafc50a001f7a0104272608',
                            'valor': item['tcecodcargo']
                        },
                        {
                            'id': '5fafc50a001f7a0104272607',
                            'valor': item['tcetipocargoacu']
                        }
                    ]
                }
            })
        if 'configuracaoferias' in item and item['configuracaoferias'] is not None:
            dict_dados['conteudo'].update({
                'configuracaoFerias': {
                    'id': item['configuracaoferias']
                }
            })
        if 'grauinstrucao' in item and item['grauinstrucao'] is not None:
            dict_dados['conteudo'].update({'grauInstrucao': item['grauinstrucao']})
        if 'situacaograuinstrucao' in item and item['situacaograuinstrucao'] is not None:
            dict_dados['conteudo'].update({'situacaoGrauInstrucao': item['situacaograuinstrucao']})
        if 'contagemespecial' in item and item['contagemespecial'] is not None:
            dict_dados['conteudo'].update({'contagemEspecial': item['contagemespecial']})
        if 'unidadepagamento' in item and item['unidadepagamento'] is not None:
            dict_dados['conteudo'].update({'unidadePagamento': item['unidadepagamento']})
        if 'quadrocargos' in item and item['quadrocargos'] is not None:
            dict_dados['conteudo'].update({'quadroCargos': item['quadrocargos']})
        if 'requisitosnecessarios' in item and item['requisitosnecessarios'] is not None:
            dict_dados['conteudo'].update({'requisitosNecessarios': item['requisitosnecessarios']})
        if 'atividadesdesempenhadas' in item and item['atividadesdesempenhadas'] is not None:
            dict_dados['conteudo'].update({'atividadesDesempenhadas': item['atividadesdesempenhadas']})
        if 'extinto' in item and item['extinto'] is not None:
            dict_dados['conteudo'].update({'extinto': item['extinto']})
        if 'configuracaolicencapremio' in item and item['configuracaolicencapremio'] is not None:
            dict_dados['conteudo'].update({'configuracaoLicencaPremio': item['configuracaolicencapremio']})        
        if 'nivelsalarial' in item and item['nivelsalarial'] is not None:
            lista_niveis = []
            for n in item['nivelsalarial']:
                dados_niveis = n.split(':')
                # print('dados_niveis', dados_niveis)                
                if not re.search(r'\?', dados_niveis[1]) and dados_niveis[1] != '0':
                    dict_item_niveis = {
                        'nivelSalarial': {
                            'id': int(dados_niveis[1])
                        }
                    }
                    lista_niveis.append(dict_item_niveis)
            # print('lista_niveis', lista_niveis)
            lista_niveis = list_unique(lista_niveis)
            dict_dados['conteudo'].update({
                'remuneracoes': lista_niveis
            })
        if 'historico' in item and item['historico'] is not None:
            lista_historico = []
            for h in item['historico'].split('%/%'):
                dados_historico = h.split(',')
                # print('dados_historico', dados_historico, type(dados_historico))
                for idx, val in enumerate(dados_historico):
                    pass
                    # print(idx, val)
                dict_item_historico = {
                    'descricao': dados_historico[8],
                    'inicioVigencia': dados_historico[9] + ' 01:00:00',
                    'pagaDecimoTerceiroSalario': dados_historico[21],
                    'contagemEspecial': dados_historico[20],
                    'acumuloCargos': dados_historico[14],
                    'quantidadeVagasPcd': dados_historico[22],
                    'extinto': dados_historico[26],
                    'grauInstrucao': dados_historico[17],
                    'quantidadeVagas': dados_historico[15],
                    'dedicacaoExclusiva': dados_historico[19],
                    'tipo': {
                        'id': dados_historico[2]
                    },
                    
                }
                if dados_historico[1] != '0':
                    dict_item_historico.update({
                        'cbo': {
                            'id': dados_historico[1]
                        }
                    })
                elif 'cbo' in item and item['cbo'] is not None:
                    dict_item_historico.update({
                            'cbo': {
                                'id': item['cbo']
                            }
                        })
                if dados_historico[0] != '0':
                    dict_item_historico.update({
                        'ato': {
                            'id': dados_historico[0]
                        }
                    })
                if 'camposadicionais' in item and item['camposadicionais'] is not None:
                    dict_dados['conteudo'].update({
                        'camposAdicionais': {
                            'tipo': 'CARGO',
                            'campos': [
                                {
                                    'id': '5fafc50a001f7a0104272606',
                                    'valor': dados_historico[26]
                                },
                                {
                                    'id': '5fafc50a001f7a0104272608',
                                    'valor': dados_historico[27]
                                },
                                {
                                    'id': '5fafc50a001f7a0104272607',
                                    'valor': dados_historico[28]
                                }
                            ]
                        }
                    })
                if dados_historico[3] != '0':
                    dict_item_historico.update({
                        'configuracaoFerias': {
                            'id': dados_historico[3]
                        }
                    })
                lista_niveis_historico = []
                if re.search(r'\:', dados_historico[27]):
                    for item_niveis in dados_historico[27].split('|'):
                        nivel = item_niveis.split(':')
                        if not re.search(r'\?', nivel[1]) and nivel[1] != '0':
                            try:
                                dict_item_niveis = {
                                    'nivelSalarial': {
                                        'id': int(nivel[1])
                                    }
                                }
                                lista_niveis_historico.append(dict_item_niveis)
                            except Exception as error:
                                print(f"Erro na geração de item {nivel[1]}. ", error)
                if len(lista_niveis_historico) > 0:
                    # print('lista_niveis_historico', lista_niveis_historico)
                    lista_niveis_historico = list_unique(lista_niveis_historico)
                    dict_item_historico.update({
                        'remuneracoes': lista_niveis_historico
                    })
                lista_historico.append(dict_item_historico)
            dict_dados['conteudo'].update({
                'historicos': lista_historico
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
            'descricao_tipo_registro': 'Cadastro de Cargos',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['entidade'],
            'i_chave_dsk2': item['codigo']
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
