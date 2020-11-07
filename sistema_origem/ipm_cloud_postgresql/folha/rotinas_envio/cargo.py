import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 300
tipo_registro = 'cargo'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/cargo'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # T - Realiza a pré-validação dos dados
    dados_enviar = pre_validar(params_exec, dados_assunto)

    # L - Realiza o envio dos dados validados
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')

    model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


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

            # INSERIR AS REGRAS DE PRÉ VALIDAÇÃO AQUI

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
        # print(f'\r- Gerando JSON: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['chave_dsk1'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'descricao': item['descricao'],
                'acumuloCargos': item['acumulocargos'],
                'quantidadeVagas': item['quantidadevagas'],
                'dedicacaoExclusiva': item['dedicacaoexclusiva'],
                'inicioVigencia': item['iniciovigencia'].strftime("%Y-%m-%d %H:%M:%S"),
                'pagaDecimoTerceiroSalario': item['pagadecimoterceirosalario'],
                'contagemEspecial': item['contagemespecial'],
                'quantidadeVagasPcd': item['quantidadevagaspcd'],
                'ato': {
                    'id': item['id_ato']
                },
                'cbo': {
                    'id': item['id_cbo']
                },
                'tipo': {
                    'id': item['id_tipo_cargo']
                }
             }
        }

        if 'configuracao_ferias' in item and item['configuracao_ferias'] is not None:
            dict_dados['conteudo'].update({
                'configuracaoFerias': {
                    'id': item['configuracao_ferias']
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

        if 'unidadepagamento' in item and item['unidadepagamento'] is not None:
            dict_dados['conteudo'].update({'unidadePagamento': item['unidadepagamento']})

        if 'quadrocargos' in item and item['quadrocargos'] is not None:
            dict_dados['conteudo'].update({'quadroCargos': item['quadrocargos']})

        if 'quadrocargos' in item and item['quadrocargos'] is not None:
            dict_dados['conteudo'].update({'quadroCargos': item['quadrocargos']})

        if 'requisitosnecessarios' in item and item['requisitosnecessarios'] is not None:
            dict_dados['conteudo'].update({'requisitosNecessarios': item['requisitosnecessarios']})

        if 'atividadesdesempenhadas' in item and item['atividadesdesempenhadas'] is not None:
            dict_dados['conteudo'].update({'atividadesDesempenhadas': item['atividadesdesempenhadas']})

        if 'extinto' in item and item['extinto'] is not None:
            dict_dados['conteudo'].update({'extinto': item['extinto']})

        if 'extinto' in item and item['extinto'] is not None:
            dict_dados['conteudo'].update({'extinto': item['extinto']})

        if 'configuracaolicencapremio' in item and item['configuracaolicencapremio'] is not None:
            dict_dados['conteudo'].update({'configuracaoLicencaPremio': item['configuracaolicencapremio']})

        if 'id_conf_ferias' in item and item['id_conf_ferias'] is not None:
            dict_dados['conteudo'].update({
                'configuracaoFerias': {
                    'id': item['id_conf_ferias']
                }
            })

        if 'niveis' in item and item['niveis'] is not None:
            lista_niveis = []
            for n in item['niveis'].split('%/%'):
                dados_niveis = n.split(';')
                dict_item_niveis = {
                    'nivelSalarial': {
                        'id': dados_niveis[1]
                    }
                }
                lista_niveis.append(dict_item_niveis)

            # print('lista_niveis', lista_niveis)
            dict_dados['conteudo'].update({
                'remuneracoes': lista_niveis
            })

        if 'historico' in item and item['historico'] is not None:
            lista_historico = []
            entradas_hist = 0
            if len(lista_historico) > 1:
                for h in item['historico'].split('%/%'):
                    entradas_hist += 1
                    dados_historico = h.split(';')
                    dict_item_historico = {
                        'descricao': dados_historico[0],
                        'inicioVigencia': dados_historico[1],
                        'pagaDecimoTerceiroSalario': dados_historico[2],
                        'contagemEspecial': dados_historico[3],
                        'acumuloCargos': dados_historico[4],
                        'quantidadeVagasPcd': dados_historico[5],
                        'extinto': dados_historico[6],
                        'grauInstrucao': dados_historico[7],
                    }
                    if entradas_hist >= 1:
                        lista_historico.append(dict_item_historico)

            if len(lista_historico) > 0:
                dict_dados['conteudo'].update({
                    'historico': lista_historico
                })

        contador += 1
        print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Cargos',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['chave_dsk1']
        })

    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=100)

        # Insere lote na tabela 'controle_migracao_lotes'
        model.insere_tabela_controle_lote(req_res)
        print('- Envio de dados finalizado.')

