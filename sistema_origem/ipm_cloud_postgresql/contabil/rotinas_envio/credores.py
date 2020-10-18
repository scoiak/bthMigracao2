import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # Verifica se existe algum lote pendente de execução
    # model.valida_lotes_enviados(params_exec)

    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # T - Realiza a pré-validação dos dados
    dados_enviar = pre_validar(params_exec, dados_assunto)

    # L - Realiza o envio dos dados validados
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')

    model.valida_lotes_enviados(params_exec, tipo_registro='credores')


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, 'credores.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')

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
            # print("dado_linha", linha, type(linha))

            # 1 - Verifica se o registro possui o campo obrigatório 'nome' preenchido
            if 'nome' not in linha:
                registro_valido = False
                registro_erros.append({
                    'registro': linha,
                    'mensagem_erro': f'O credor de id {linha["id"]} não possui o campo "nome" preenchido.',
                    'mensagem_ajuda': f' Acessar o cadastro do credor {linha["id"]} e inserir um nome.'
                })

            # 2 - Verifica se o registro possui o campo obrigatório 'cpfcnpj' preenchido
            if 'cpfcnpj' not in linha:
                registro_valido = False
                registro_erros.append({
                    'registro': linha,
                    'mensagem_erro': f'O credor de id {linha["id"]} não possui o campo "cpfcnpj" preenchido.',
                    'mensagem_ajuda': f' Acessar o cadastro do credor {linha["id"]} e inserir um cpf/cnpj.'
                })

            # 3 - Verifica se o campo cpfcnpj está no formato esperado
            if 'cpfcnpj' in linha:
                if len(linha['cpfcnpj']) not in [11, 14]:
                    registro_valido = False
                    registro_erros.append({
                        'registro': linha,
                        'mensagem_erro': f'O campo cpfcnpj do credor {linha["id"]} não possui a quantidade de '
                                         f'caracteres esperada.',
                        'mensagem_ajuda': f' Acessar o cadastro do credor {linha["id"]} e corrigir o cpf/cnpj.'
                    })

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
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    url = "https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/credores"

    for item in dados:
        hash_chaves = model.gerar_hash_chaves(item['chave_1'], item["chave_2"])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'content': {
                'nome': item['nome'],
                'tipo': 'FISICA' if item['tipo'] == 'FISICA' else 'JURIDICA',
                'dataInclusao': hoje
            }
        }
        if item['tipo'] == 'FISICA':
            dict_dados['content'].update({
                'fisica': {
                    'cpf': item['cpfcnpj']
                }
            })
        else:
            dict_dados['content'].update({
                'juridica': {
                    'cnpj': item['cpfcnpj']
                }
            })
        lista_dados_enviar.append(dict_dados)

        # Insere registro atual na tabela 'controle_migracao_registro'
        model.insere_tabela_controle_migracao_registro(params_exec, req_res={
            'sistema': 1,
            'tipo_registro': 'credores',
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Credores',
            'id_gerado': None,
            'i_chave_dsk1': item['chave_1'],
            'i_chave_dsk2': item['chave_2']
        })

    req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro='credores',
                                                  tamanho_lote=50)

    # Insere lote na tabela 'controle_migracao_lotes'
    model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')
