import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
from datetime import datetime

sistema = 300
tipo_registro = 'periodo-aquisitivo-ferias'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/periodo-aquisitivo-ferias'
limite_lote = 1000


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if False:
        busca_dados(params_exec)
    if True:
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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, '56', item['matricula']['id'], item['dataInicial'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Bairro',
            'id_gerado': item['id'],
            'i_chave_dsk1': '56',
            'i_chave_dsk2': item['matricula']['id'],
            'i_chave_dsk3': item['dataInicial']
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
        print(f'\r- Gerando JSON: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['entidade'], item['matricula'], item['datainicial'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'situacao': item['situacao'],
                'dataInicial': item['datainicial'],
                'dataFinal': item['datafinal'],
                'configuracaoFerias': {
                    'id': item['configuracaoferias']
                },
                'matricula': {
                    'id': item['matricula']
                },
                'faltas': item['faltas'],
                'diasAdquiridos': item['diasadquiridos'],
                'cancelados': item['cancelados'],
                'suspensos': item['suspensos'],
                'saldo': item['saldo'],
            }
        }
        if 'pagou1tercointegral' in item and item['pagou1tercointegral'] is not None:
            dict_dados['conteudo'].update({'pagou1TercoIntegral': item['pagou1tercointegral']})
        if 'diasanuladosrescisao' in item and item['diasanuladosrescisao'] is not None:
            dict_dados['conteudo'].update({'diasAnuladosRescisao': item['diasanuladosrescisao']})
        if 'conversao' in item and item['conversao'] is not None:
            dict_dados['conteudo'].update({'conversao': item['conversao']})
        if 'competenciafechamentoprovisao' in item and item['competenciafechamentoprovisao'] is not None:
            dict_dados['conteudo'].update({'competenciaFechamentoProvisao': item['competenciafechamentoprovisao']})
        if item['movimentacoes'] is not None:
            listaconteudo = []
            lista = item['movimentacoes'].split('%||%')
            if len(lista) > 0:
                for listacampo in lista:
                    campo = listacampo.split('%|%')
                    for idx, val in enumerate(campo):
                        if campo[idx] == '':
                            campo[idx] = None
                    dict_item_conteudo = {}
                    if campo[16] is not None:
                        dict_item_conteudo.update({
                            'conversao': campo[16]
                        })
                    if False:
                        if campo[1] is not None:
                            dict_item_conteudo.update({
                                'dataInicio': campo[1]
                            })
                    if False:
                        if campo[2] is not None:
                            dict_item_conteudo.update({
                                'dataFim': campo[2]
                            })
                    if campo[3] is not None:
                        dict_item_conteudo.update({
                            'motivo': campo[3]
                        })
                    if campo[4] is not None:
                        dict_item_conteudo.update({
                            'dataCancelamento': campo[4]
                        })
                    if campo[5] is not None:
                        dict_item_conteudo.update({
                            'dataInicioGozo': campo[5]
                        })
                    if campo[6] is not None:
                        dict_item_conteudo.update({
                            'dataFimGozo': campo[6]
                        })
                    if campo[7] is not None:
                        dict_item_conteudo.update({
                            'dataPagamento': campo[7]
                        })
                    if campo[8] is not None:
                        dict_item_conteudo.update({
                            'diasAbono': campo[8]
                        })
                    if campo[9] is not None:
                        dict_item_conteudo.update({
                            'diasGozo': campo[9]
                        })
                    if campo[10] is not None:
                        dict_item_conteudo.update({
                            'diasFalta': campo[10]
                        })
                    if campo[11] is not None:
                        dict_item_conteudo.update({
                            'diasPagosRescisao': campo[11]
                        })
                    if campo[12] is not None:
                        dict_item_conteudo.update({
                            'ferias': campo[12]
                        })
                    if campo[13] is not None:
                        dict_item_conteudo.update({
                            'tipo': campo[13]
                        })
                    if campo[14] is not None:
                        dict_item_conteudo.update({
                            'dataMovimento': campo[14]
                        })
                    if campo[15] is not None:
                        dict_item_conteudo.update({
                            'quantidade': campo[15]
                        })
                    listaconteudo.append(dict_item_conteudo)
            if len(listaconteudo) > 0:
                dict_dados['conteudo'].update({
                    'movimentacoes': listaconteudo
                })
        print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Lotação Física',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['entidade'],
            'i_chave_dsk2': item['matricula'],
            'i_chave_dsk3': item['datainicial']
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