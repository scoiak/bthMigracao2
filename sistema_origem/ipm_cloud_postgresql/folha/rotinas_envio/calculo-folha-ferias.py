from datetime import datetime
import re
import json
import logging
import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

sistema = 300
tipo_registro = 'calculo-folha-ferias'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/calculo-folha-ferias'
limite_lote = 1000


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if True:
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
        for matricula in item['calculoFolhaMatriculas']:
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, params_exec.get('entidade'), matricula['matricula']['id'],
                                                  item['tipoProcessamento'], item['subTipoProcessamento'],
                                                  item['dataPagamento'])
            registros_formatados.append({
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro do Calculo da Folha de Ferias',
                'id_gerado': item['id'],
                'i_chave_dsk1': params_exec.get('entidade'),
                'i_chave_dsk2': matricula['matricula']['id'],
                'i_chave_dsk3': item['tipoProcessamento'],
                'i_chave_dsk4': item['subTipoProcessamento'],
                'i_chave_dsk5': item['dataPagamento']
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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['entidade'], item['matricula'], item['tipoprocessamento'], item['subtipoprocessamento'], item['datapagamento'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'calculoFolhaMatriculas': [
                    {
                        'matricula': {
                            'id': item['matricula']
                        },
                        'saldoFgts': item['saldofgts'],
                        'fgtsMesAnterior': item['fgtsmesanterior'],
                        'periodos': [
                            {
                                "id": item['periodos']
                            }
                        ]
                    }
                ]
            }
        }
        if 'tipoprocessamento' in item and item['tipoprocessamento'] is not None:
            dict_dados['conteudo'].update({'tipoProcessamento': item['tipoprocessamento']})
        if 'subtipoprocessamento' in item and item['subtipoprocessamento'] is not None:
            dict_dados['conteudo'].update({'subTipoProcessamento': item['subtipoprocessamento']})
        if 'dataagendamento' in item and item['dataagendamento'] is not None:
            dict_dados['conteudo'].update({'dataAgendamento': item['dataagendamento']})
        if 'datapagamento' in item and item['datapagamento'] is not None:
            dict_dados['conteudo'].update({'dataPagamento': item['datapagamento']})
        if 'tipovinculacaomatricula' in item and item['tipovinculacaomatricula'] is not None:
            dict_dados['conteudo'].update({'tipoVinculacaoMatricula': item['tipovinculacaomatricula']})
        if 'conversao' in item and item['conversao'] is not None:
            dict_dados['conteudo'].update({'conversao': item['conversao']})
        if 'diasabono' in item and item['diasabono'] is not None:
            dict_dados['conteudo'].update({'diasAbono': item['diasabono']})
        if 'diasgozo' in item and item['diasgozo'] is not None:
            dict_dados['conteudo'].update({'diasGozo': item['diasgozo']})
        if 'datainicial' in item and item['datainicial'] is not None:
            dict_dados['conteudo'].update({'dataInicial': item['datainicial']})
        if 'datafinal' in item and item['datafinal'] is not None:
            dict_dados['conteudo'].update({'dataFinal': item['datafinal']})
        if 'coletiva' in item and item['coletiva'] is not None:
            dict_dados['conteudo'].update({'coletiva': item['coletiva']})
        if 'pagarumtercointegral' in item and item['pagarumtercointegral'] is not None:
            dict_dados['conteudo'].update({'pagarUmTercoIntegral': item['pagarumtercointegral']})
        if 'observacoes' in item and item['observacoes'] is not None:
            dict_dados['conteudo'].update({'observacoes': item['observacoes']})
        if 'tipocalculoferias' in item and item['tipocalculoferias'] is not None:
            dict_dados['conteudo'].update({'tipoCalculoFerias': item['tipocalculoferias']})
        if 'descontarfaltas' in item and item['descontarfaltas'] is not None:
            dict_dados['conteudo'].update({'descontarFaltas': item['descontarfaltas']})
        if 'anodecimoterceiro' in item and item['anodecimoterceiro'] is not None:
            dict_dados['conteudo'].update({'anoDecimoTerceiro': item['anodecimoterceiro']})
        if 'consideraavosperdidosdecimoterceiro' in item and item['consideraavosperdidosdecimoterceiro'] is not None:
            dict_dados['conteudo'].update({'consideraAvosPerdidosDecimoTerceiro': item['consideraavosperdidosdecimoterceiro']})
        if 'saldo' in item and item['saldo'] is not None:
            dict_dados['conteudo'].update({'saldo': item['saldo']})
        if 'diasparagozo' in item and item['diasparagozo'] is not None:
            dict_dados['conteudo'].update({'diasParaGozo': item['diasparagozo']})
        if 'ato' in item and item['ato'] is not None:
            dict_dados['conteudo'].update({
                'ato': {
                    'id': int(item['ato'])
                }})
        if 'tipoafastamento' in item and item['tipoafastamento'] is not None:
            dict_dados['conteudo'].update({
                'tipoAfastamento': {
                    'id': int(item['tipoafastamento'])
                }})
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
            'descricao_tipo_registro': 'Cadastro do Calculo da Folha de Ferias',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['entidade'],
            'i_chave_dsk2': item['matricula'],
            'i_chave_dsk3': item['tipoprocessamento'],
            'i_chave_dsk4': item['subtipoprocessamento'],
            'i_chave_dsk5': item['datapagamento']
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
