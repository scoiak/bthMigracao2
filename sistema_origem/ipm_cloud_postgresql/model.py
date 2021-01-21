from datetime import datetime
import hashlib
import time
import logging
import re
import requests
import pandas as pd
import psycopg2
import settings
class PostgreSQLConnection:
    conn = None

    def __init__(self):
        self.connect()

    def connect(self, **kwargs):
        try:
            logging.info('Iniciando execução com o banco de dados.')
            self.conn = psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                dbname=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PW)

        except (Exception, psycopg2.DatabaseError) as error:
            print("Erro ao executar função 'PostgreSQLConnection:connect'.", error)

    def exec_sql(self, sql, **kwargs):
        dataframe = None
        try:
            if self.conn is not None:
                if 'index_col' in kwargs:
                    dataframe = pd.read_sql_query(sql, self.conn, index_col=kwargs['index_col'])
                else:
                    dataframe = pd.read_sql_query(sql, self.conn)
        except Exception as error:
            print("Erro ao executar função 'PostgreSQLConnection:exec_sql'.", error)
        finally:
            return dataframe

    def exec_insert(self, sql, **kwargs):
        try:
            if self.conn is not None:
                cursor = self.conn.cursor()
                result = cursor.execute(sql)
                if result is not None:
                    print(result)
                self.conn.commit()
        except Exception as error:
            print("Erro ao executar função 'PostgreSQLConnection:configura_banco'.", error)

    def close_connection(self):
        try:
            self.conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Erro ao executar função 'PostgreSQLConnection:close_connection'.", error)

    def verifica_tabelas_controle(self):
        try:
            logging.info('Verificando existência de tabelas de controle de migração.')
            df = self.exec_sql("SELECT 1 "
                               "FROM information_schema.tables "
                               "WHERE table_schema = 'public' "
                               "AND table_name = 'controle_migracao_registro'")
            existe_tabelas_controle = not df.empty
            if not existe_tabelas_controle:
                logging.info('Tabelas de controle não encontradas.')
                self.configura_banco()
            else:
                logging.info('Tabelas de controle encontradas.')
        except Exception as error:
            print("Erro ao executar função 'PostgreSQLConnection:verifica_tabelas_controle'.", error)

    def configura_banco(self):
        logging.info('Iniciando configuração no banco de dados PostgreSQL.')
        print("Iniciando configuração no banco de dados PostgreSQL.")
        try:
            path = f"sistema_origem/{settings.BASE_ORIGEM}/{settings.SISTEMA_ORIGEM}/sql_padrao/config.sql"
            commands = open(path, "r").read().split("%/%")
            for sql in commands:
                if sql != "":
                    cursor = self.conn.cursor()
                    result = cursor.execute(sql)
                    if result is not None:
                        print(result)
                    self.conn.commit()
            logging.info('Configuração efetuada com sucesso.')
            print("Configuração efetuada com sucesso.")
        except Exception as error:
            print("Erro ao executar função 'PostgreSQLConnection:configura_banco'.", error)


def get_path(assunto):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/{settings.SISTEMA_ORIGEM}/sql_padrao/'
    path_custom = f'sistema_origem/ipm_cloud_postgresql/{settings.SISTEMA_ORIGEM}/sql_custom/'
    # existe_customizado = os.path.isfile(path_custom + assunto)
    existe_customizado = False
    path = (path_custom if existe_customizado else path_padrao) + assunto
    return path


def aplica_parametros(params_exec, t):
    texto_consulta = t
    try:
        for param in params_exec:
            texto_consulta = texto_consulta.replace(('{{' + param + '}}'), str(params_exec.get(param)))

    except Exception as error:
        print("Erro ao executar função 'aplica_parametros'.", error)

    finally:
        return texto_consulta


def get_consulta(params_exec, assunto):
    texto_consulta = None
    try:
        logging.info(f'Iniciando busca de consulta para o assunto {assunto}.')

        # Obtém o texto do arquivo assunto.sql na pasta 'sql_padrao'
        texto_consulta = open(get_path(f'{assunto}'), "r", encoding='utf-8').read()

        # Aplica os parâmetros de usuário na consulta obtida
        texto_consulta = aplica_parametros(params_exec, texto_consulta)

    except Exception as error:
        print("Erro ao executar função 'get_consulta'.", error)

    finally:
        return texto_consulta


def gerar_hash_chaves(*args):
    chaves = ''
    for item in args:
        chaves += str(item)
    hash_chaves = hashlib.md5(chaves.encode('utf-8')).hexdigest()
    return hash_chaves


def insere_tabela_controle_lote(req_res):
    pgcnn = None
    logging.info('Inserindo daods na tabela de controle de lotes.')
    if req_res is not None and len(req_res) != 0:
        try:
            pgcnn = PostgreSQLConnection()
            for item_retorno in req_res:
                sql = 'INSERT INTO public.controle_migracao_lotes ' \
                      '(i_sequencial, sistema, tipo_registro, data_hora_env, usuario, url_consulta, status, ' \
                      'id_lote, conteudo_json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cursor = pgcnn.conn.cursor()
                result = cursor.execute(sql, (0, item_retorno['sistema'], item_retorno['tipo_registro'],
                                              item_retorno['data_hora_envio'], item_retorno['usuario'],
                                              item_retorno['url_consulta'], item_retorno['status'],
                                              item_retorno['id_lote'], item_retorno['conteudo_json']))
                pgcnn.conn.commit()
        except Exception as error:
            print("Erro ao executar função 'insere_tabela_controle_lote'.", error)

        finally:
            pgcnn.close_connection()


def atualiza_tabela_controle_lote(**kwargs):
    result = None
    pgcnn = None

    try:
        pgcnn = PostgreSQLConnection()
        cursor = pgcnn.conn.cursor()
        result = cursor.execute(kwargs.get('sql'))
        pgcnn.conn.commit()

    except Exception as error:
        # print("Erro ao executar função 'atualiza_tabela_controle_lote'.", result, error)
        pass

    finally:
        pgcnn.close_connection()


def insere_tabela_controle_registro_ocor(req_res):
    pgcnn = None
    logging.info('Inserindo dados na tabela de controle de ocorrências.')
    if req_res is not None and len(req_res) != 0:
        try:
            pgcnn = PostgreSQLConnection()
            for item_retorno in req_res:
                sql = 'INSERT INTO public.controle_migracao_registro_ocor ' \
                      '(i_sequencial, hash_chave_dsk, sistema, tipo_registro, id_gerado, origem, situacao, resolvido,' \
                      'i_sequencial_lote, id_integracao, mensagem_erro, mensagem_ajuda, json_enviado, id_existente) ' \
                      'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cursor = pgcnn.conn.cursor()
                result = cursor.execute(sql, item_retorno)
                pgcnn.conn.commit()
        except Exception as error:
            print("Erro ao executar função 'insere_tabela_controle_registro_ocor'.", error)

        finally:
            pgcnn.close_connection()


def insere_tabela_controle_migracao_registro(params_exec, lista_req):
    logging.info('Inserindo dados na tabela de controle de registros.')
    pgcnn = None
    itens_por_insert = 200
    data_list = []
    sql = 'INSERT INTO public.controle_migracao_registro ' \
          '(sistema, tipo_registro, hash_chave_dsk, descricao_tipo_registro, id_gerado, i_chave_dsk1, ' \
          'i_chave_dsk2, i_chave_dsk3, i_chave_dsk4, i_chave_dsk5, i_chave_dsk6, i_chave_dsk7, i_chave_dsk8,' \
          'i_chave_dsk9, i_chave_dsk10, i_chave_dsk11, i_chave_dsk12, json_enviado) ' \
          'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ' \
          ' ON CONFLICT (hash_chave_dsk) DO UPDATE SET json_enviado = EXCLUDED.json_enviado, id_gerado = %s'

    if lista_req is not None:
        try:
            # Converte os dicts de dados em tuples
            for item in lista_req:
                # print('item', type(item), item)
                values = (
                    item.get('sistema'),
                    item.get('tipo_registro'),
                    item.get('hash_chave_dsk'),
                    item.get('descricao_tipo_registro'),
                    item.get('id_gerado'),
                    item.get('i_chave_dsk1'),
                    None if 'i_chave_dsk2' not in item else item.get('i_chave_dsk2'),
                    None if 'i_chave_dsk3' not in item else item.get('i_chave_dsk3'),
                    None if 'i_chave_dsk4' not in item else item.get('i_chave_dsk4'),
                    None if 'i_chave_dsk5' not in item else item.get('i_chave_dsk5'),
                    None if 'i_chave_dsk6' not in item else item.get('i_chave_dsk6'),
                    None if 'i_chave_dsk7' not in item else item.get('i_chave_dsk7'),
                    None if 'i_chave_dsk8' not in item else item.get('i_chave_dsk8'),
                    None if 'i_chave_dsk9' not in item else item.get('i_chave_dsk9'),
                    None if 'i_chave_dsk10' not in item else item.get('i_chave_dsk10'),
                    None if 'i_chave_dsk11' not in item else item.get('i_chave_dsk11'),
                    None if 'i_chave_dsk12' not in item else item.get('i_chave_dsk12'),
                    None if 'json' not in item else item.get('json'),
                    None if 'id_gerado' not in item else item.get('id_gerado')
                )
                data_list.append(values)

            # Instancia conexão
            pgcnn = PostgreSQLConnection()
            cursor = pgcnn.conn.cursor()

            # Divide a lista de tuples em sublistas
            list_slice = ([data_list[i:i + itens_por_insert] for i in range(0, len(data_list), itens_por_insert)])

            # Realiza a inserção das sublistas no banco
            for item in list_slice:
                cursor.executemany(sql, item)
                pgcnn.conn.commit()

        except Exception as error:
            print("Erro ao executar função 'insere_tabela_controle_migracao_registro'.", error)

        finally:
            pgcnn.close_connection()


def insere_tabela_controle_migracao_registro2(params_exec, lista_req):
    pgcnn = None
    logging.info('Inserindo dados na tabela de controle de registros.')
    if lista_req is not None:
        pgcnn = PostgreSQLConnection()

        sql = 'INSERT INTO public.controle_migracao_registro ' \
              '(sistema, tipo_registro, hash_chave_dsk, descricao_tipo_registro, id_gerado, i_chave_dsk1, ' \
              'i_chave_dsk2, i_chave_dsk3, i_chave_dsk4, i_chave_dsk5, i_chave_dsk6, i_chave_dsk7, i_chave_dsk8,' \
              'i_chave_dsk9, i_chave_dsk10, i_chave_dsk11, i_chave_dsk12) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ' \
              ' ON CONFLICT DO NOTHING'
        try:
            for item in lista_req:
                cursor = pgcnn.conn.cursor()
                values = (
                    item.get('sistema'),
                    item.get('tipo_registro'),
                    item.get('hash_chave_dsk'),
                    item.get('descricao_tipo_registro'),
                    item.get('id_gerado'),
                    item.get('i_chave_dsk1'),
                    None if 'i_chave_dsk2' not in item else item.get('i_chave_dsk2'),
                    None if 'i_chave_dsk3' not in item else item.get('i_chave_dsk3'),
                    None if 'i_chave_dsk4' not in item else item.get('i_chave_dsk4'),
                    None if 'i_chave_dsk5' not in item else item.get('i_chave_dsk5'),
                    None if 'i_chave_dsk6' not in item else item.get('i_chave_dsk6'),
                    None if 'i_chave_dsk7' not in item else item.get('i_chave_dsk7'),
                    None if 'i_chave_dsk8' not in item else item.get('i_chave_dsk8'),
                    None if 'i_chave_dsk9' not in item else item.get('i_chave_dsk9'),
                    None if 'i_chave_dsk10' not in item else item.get('i_chave_dsk10'),
                    None if 'i_chave_dsk11' not in item else item.get('i_chave_dsk11'),
                    None if 'i_chave_dsk12' not in item else item.get('i_chave_dsk12')
                )
                result = cursor.execute(sql, values)
                pgcnn.conn.commit()

        except Exception as error:
            print("Erro ao executar função 'insere_tabela_controle_migracao_registro2'.", error)

        finally:
            pgcnn.close_connection()


def atualiza_tabelas_controle_envio_sem_lote(params_exec, req_res, *args, **kwargs):
    lista_inconsistencia = []
    dados_inserir_ocor = []

    try:
        for item in req_res:
            if item['id_gerado'] is None:
                dados_inserir_ocor.append((0, item['hash_chave'], get_codigo_sistema(),
                                           kwargs.get('tipo_registro'), None, 9, 9,
                                           9, 1, item['hash_chave'],
                                           item['mensagem'], '', '', None))
            else:
                atualiza_controle_migracao_registro(item['id_gerado'], item['hash_chave'])

        insere_tabela_controle_registro_ocor(dados_inserir_ocor)

    except Exception as error:
        print("Erro ao executar função 'atualiza_tabelas_controle_envio_sem_lote'.", error)


def atualiza_controle_migracao_registro(id_gerado, hash_chave):
    pgcnn = None
    try:
        pgcnn = PostgreSQLConnection()
        sql = f'UPDATE public.controle_migracao_registro ' \
              f'SET id_gerado = {id_gerado} ' \
              f'WHERE hash_chave_dsk = \'{hash_chave}\';'
        pgcnn.exec_insert(sql)

    except Exception as error:
        print("Erro ao executar função 'atualiza_controle_migracao_registro'.", error)


def valida_lotes_enviados(params_exec, *args, **kwargs):
    print('- Iniciando validação de lotes enviados pendentes.')
    dh_inicio = datetime.now()
    pgcnn = None
    lotes_validados = 0
    total_lotes = 0
    retorno_analise_lote = {
        'incosistencia_registros': 0,
        'incosistencia_lotes': 0
    }

    try:
        pgcnn = PostgreSQLConnection()
        existe_pendencia = True

        while existe_pendencia:
            slq = 'SELECT id_lote, url_consulta FROM public.controle_migracao_lotes WHERE status not in (3, 4, 5)'
            if 'tipo_registro' in kwargs:
                slq += f" AND tipo_registro = '{kwargs.get('tipo_registro')}'"
            df = pgcnn.exec_sql(slq)
            existe_pendencia = False
            if total_lotes == 0:
                total_lotes = len(df)

            if len(df) == 0:
                print('- Não restam lotes pendentes para validação.')
            else:
                # Inicia rodadas de verificação de lotes pendentes
                agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                # print(f'- Verificando {len(df)} lote(s) pendente(s) ({agora}).')
                headers = {'authorization': f'bearer {params_exec["token"]}', 'content-type': 'application/json'}

                # Se existir lote spendentes, iniciar o loop de consultas
                for lote in df.to_dict('records'):
                    print(f'\r- Lotes executados: {lotes_validados}/{total_lotes}', end='')
                    url = lote['url_consulta']
                    req = requests.get(url, headers=headers)
                    if 'json' in req.headers.get('Content-Type'):
                        retorno_json = req.json()

                        if 'id' in retorno_json:
                            id_lote = retorno_json['id']
                        elif 'idLote' in retorno_json:
                            id_lote = retorno_json['idLote']
                        else:
                            id_lote = ''

                        if 'status' in retorno_json:
                            status = retorno_json['status']
                        elif 'situacao' in retorno_json:
                            status = retorno_json['situacao']
                        else:
                            status = ''

                        if status in ['AGUARDANDO_EXECUCAO', 'EXECUTANDO']:
                            existe_pendencia = True
                        else:
                            retorno_analise_lote_novo = analisa_retorno_lote(params_exec,
                                                                        retorno_json,
                                                                        id_lote=id_lote,
                                                                        tipo_registro=kwargs.get('tipo_registro'))
                            retorno_analise_lote["incosistencia_registros"] += retorno_analise_lote_novo["incosistencia_registros"]
                            retorno_analise_lote["incosistencia_lotes"] += retorno_analise_lote_novo["incosistencia_lotes"]
                            lotes_validados += 1
                            print(f'\r- Lotes executados: {lotes_validados}/{total_lotes}', end='')
                    else:
                        print('Retorno não JSON:', req.status_code, req.text)

            if existe_pendencia:
                time.sleep(5)

        if retorno_analise_lote["incosistencia_registros"] > 0:
            print(f'\n- {retorno_analise_lote["incosistencia_registros"]} registro(s) retornaram inconsistência. ')
        else:
            print('\n- Nenhuma inconsistência encontrada nos lotes enviados.')
        print(f'- Consulta de lotes finalizada. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    except Exception as error:
        print("Erro ao executar função 'valida_lotes_enviados'.", error)

    finally:
        pgcnn.close_connection()


def analisa_retorno_lote(params_exec, retorno_json, **kwargs):
    resultado_analise = {
        'incosistencia_registros': 0,
        'incosistencia_lotes': 0
    }
    status_lote = None
    dados_atualizar_controle_reg = []
    dados_inserir_ocor = []

    try:
        # Obtém status de execução do lote
        if 'status' in retorno_json:
            status_lote = retorno_json['status']
        elif 'situacao' in retorno_json:
            status_lote = retorno_json['situacao']

        # Seta o valor do statuso do lote
        if status_lote in ['EXECUTADO', 'PROCESSADO']:
            status_lote = 3
        elif status_lote in ['EXECUTADO_PARCIALMENTE']:
            status_lote = 4
        else:
            status_lote = 5

        # Atualiza contagem de inconsistências
        if status_lote in [4, 5]:
            resultado_analise['incosistencia_lotes'] += 1

        # Obtém o id do lote retornado
        if 'id' in retorno_json:
            id_registro = retorno_json["id"]
        elif 'idGerado' in retorno_json:
            id_registro = retorno_json["id"]
        else:
            id_registro = ''

        # Armazena o horário de finalização de execução do lote
        if 'updatedIn' in retorno_json:
            if re.match('\\d{2}\\.\\d+$', retorno_json['updatedIn']):
                data_hora_ret = datetime.strptime(retorno_json['updatedIn'], '%Y-%m-%dT%H:%M:%S.%f')
            elif re.match('\\d{2}\\:\\d{2}\\:\\d{2}$', retorno_json['updatedIn']):
                data_hora_ret = datetime.strptime(retorno_json['updatedIn'], '%Y-%m-%dT%H:%M:%S')
            else:
                data_hora_ret = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            data_hora_ret = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Atualiza o status do lote na tabela de controle
        sql = f'UPDATE public.controle_migracao_lotes ' \
              f'SET status = {status_lote}, ' \
              f'    data_hora_ret = \'{data_hora_ret}\' ' \
              f'WHERE id_lote = \'{id_registro}\''
        atualiza_tabela_controle_lote(sql=sql)

        # Analisa registros dos registros contidos no lote
        if 'retorno' in retorno_json:
            for registro in retorno_json['retorno']:
                # Obtém o status de retorno do registro
                if 'status' in registro:
                    status = registro['registro']
                elif 'situacao' in registro:
                    status = registro['situacao']
                else:
                    status = ''

                # Ação para o registro migrado com sucesso
                if status in ['SUCESSO', 'SUCESS', 'EXECUTADO']:
                    registro_status = 3
                    registro_resolvido = 2
                    id_gerado = registro['idGerado']
                    hash_chave = registro['idIntegracao']
                    if id_gerado is not None and hash_chave is not None:
                        dados_atualizar_controle_reg.append([id_gerado, hash_chave])
                        # atualiza_controle_migracao_registro(id_gerado, hash_chave)

                # Ação para registro já cadastrado no cloud
                elif status == 'ERRO' and 'idExistente' in registro:
                    if registro['idExistente'] is not None:
                        registro_status = 3
                        registro_resolvido = 2
                        # para o contábil, a linha abaixo deve ser registro['idExistente'][0]
                        id_gerado = registro['idExistente']
                        hash_chave = registro['idIntegracao']
                        if id_gerado is not None and hash_chave is not None:
                            dados_atualizar_controle_reg.append([id_gerado, hash_chave])
                            # atualiza_controle_migracao_registro(id_gerado, hash_chave)
                    else:
                        resultado_analise['incosistencia_registros'] += 1
                        registro_status = 1
                        registro_resolvido = 1
                        registro_mensagem = '' if 'mensagem' not in registro else registro['mensagem']
                        id_existente = None if 'idExistente' not in registro else registro['idExistente']

                        # No desktop, existe uma proc chamada 'dbf_atualiza_controle_migracao_registro_integ'
                        # que faz a atualização da tabela _ocor e _registro simultaneamente, verificar
                        dados_inserir_ocor.append((0, registro['idIntegracao'], get_codigo_sistema(),
                                                   kwargs.get('tipo_registro'), None, 9, registro_status,
                                                   registro_resolvido, 1, kwargs.get('id_lote'),
                                                   registro['mensagem'], '', '', id_existente))

                # Ação para registro não cadastrado devido a erro
                else:
                    resultado_analise['incosistencia_registros'] += 1
                    registro_status = 1
                    registro_resolvido = 1
                    registro_mensagem = '' if 'mensagem' not in registro else registro['mensagem']
                    id_existente = None if 'idExistente' not in registro else registro['idExistente']

                    # No desktop, existe uma proc chamada 'dbf_atualiza_controle_migracao_registro_integ'
                    # que faz a atualização da tabela _ocor e _registro simultaneamente, verificar
                    dados_inserir_ocor.append((0, registro['idIntegracao'], get_codigo_sistema(),
                                               kwargs.get('tipo_registro'), None, 9, registro_status,
                                               registro_resolvido, 1, kwargs.get('id_lote'),
                                               registro['mensagem'], '', '', id_existente))

        insere_tabela_controle_registro_ocor(dados_inserir_ocor)
        atualiza_dados_controle_migracao(lista_dados=dados_atualizar_controle_reg)
    except Exception as error:
        print("Erro ao executar função 'analisa_retorno_lote'.", error)
    finally:
        return resultado_analise


def atualiza_dados_controle_migracao(lista_dados):
    itens_por_insert = 500
    sql = 'UPDATE public.controle_migracao_registro ' \
          'SET id_gerado = %s WHERE hash_chave_dsk = %s'

    try:
        data_list = []
        for item in lista_dados:
            data_list.append((item[0], item[1]))

        pgcnn = PostgreSQLConnection()
        cursor = pgcnn.conn.cursor()
        list_slice = ([data_list[i:i + itens_por_insert] for i in range(0, len(data_list), itens_por_insert)])

        for item in list_slice:
            cursor.executemany(sql, item)
            pgcnn.conn.commit()
    except Exception as error:
        print("Erro ao executar função 'atualiza_dados_controle_migracao'.", error)


def get_codigo_sistema():
    desc_sistema = settings.SISTEMA_ORIGEM

    if desc_sistema == 'contabil':
        cod_sistema = 1
    elif desc_sistema == 'folha':
        cod_sistema = 300
    elif desc_sistema == 'tributos':
        cod_sistema = 301
    elif desc_sistema == 'patrimonio':
        cod_sistema = 302
    elif desc_sistema == 'protocolo':
        cod_sistema = 304
    elif desc_sistema == 'compras':
        cod_sistema = 305
    elif desc_sistema == 'contratos':
        cod_sistema = 305
    elif desc_sistema == 'frotas':
        cod_sistema = 306
    elif desc_sistema == 'estoque':
        cod_sistema = 307
    elif desc_sistema == 'educacao':
        cod_sistema = 308
    elif desc_sistema == 'escola':
        cod_sistema = 317
    elif desc_sistema == 'controle-financeiro':
        cod_sistema = 324
    elif desc_sistema == 'faturamento-agua':
        cod_sistema = 309
    elif desc_sistema == 'biblioteca':
        cod_sistema = 320
    elif desc_sistema == 'plurianual':
        cod_sistema = 311
    elif desc_sistema == 'legislacao':
        cod_sistema = 312
    elif desc_sistema == 'rh':
        cod_sistema = 314
    elif desc_sistema == 'tesouraria':
        cod_sistema = 318
    elif desc_sistema == 'proposta':
        cod_sistema = 319
    elif desc_sistema == 'planejamento':
        cod_sistema = 325
    elif desc_sistema == 'ponto':
        cod_sistema = 329
    elif desc_sistema == 'validador':
        cod_sistema = 331
    elif desc_sistema == 'procuradoria':
        cod_sistema = 335
    elif desc_sistema == 'legislativo':
        cod_sistema = 303
    elif desc_sistema == 'producao-primaria':
        cod_sistema = 340
    elif desc_sistema == 'sapo-utilitario':
        cod_sistema = 341
    else:
        cod_sistema = 999
    return cod_sistema


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext
