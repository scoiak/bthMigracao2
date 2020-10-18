import pandas as pd
import psycopg2
import settings
import hashlib
import requests
import time
from datetime import datetime


class PostgreSQLConnection:
    conn = None

    def __init__(self):
        self.connect()

    def connect(self, **kwargs):
        try:
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

    def close_connection(self):
        try:
            self.conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Erro ao executar função 'PostgreSQLConnection:close_connection'.", error)

    def verifica_tabelas_controle(self):
        try:
            df = self.exec_sql("SELECT 1 " 
                               "FROM information_schema.tables "
                               "WHERE table_schema = 'public' "
                               "AND table_name = 'controle_migracao_registro'")
            existe_tabelas_controle = not df.empty
            if not existe_tabelas_controle:
                self.configura_banco()
        except Exception as error:
            print("Erro ao executar função 'PostgreSQLConnection:verifica_tabelas_controle'.", error)

    def configura_banco(self):
        print("Iniciando configuração no banco de dados PostgreSQL.")
        try:
            commands = open("sistema_origem/postgres/sql_padrao/config.sql", "r").read().split("%/%")
            for sql in commands:
                if sql != "":
                    cursor = self.conn.cursor()
                    result = cursor.execute(sql)
                    print(result)
                    self.conn.commit()
            print("Configuração efetuada com sucesso.")
        except Exception as error:
            print("Erro ao executar função 'PostgreSQLConnection:configura_banco'.", error)


def get_path(assunto):
    path_padrao = 'sistema_origem/ipm_cloud_postgresql/contabil/sql_padrao/'
    path_custom = 'sistema_origem/ipm_cloud_postgresql/contabil/sql_custom/'
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
        # Obtém o texto do arquivo assunto.sql na pasta 'sql_padrao'
        texto_consulta = open(get_path(f'{assunto}'), "r").read()

        # Aplica os parâmetros de usuário na consulta obtida
        texto_consulta = aplica_parametros(params_exec, texto_consulta)

    except Exception as error:
        print("Erro ao executar função 'get_consulta'.", error)

    finally:
        return texto_consulta


def gerar_hash_chaves(*args):
    chaves = f'{args[0]}'
    hash_chaves = hashlib.md5(chaves.encode('utf-8')).hexdigest()
    return hash_chaves


def insere_tabela_controle_lote(req_res):
    pgcnn = None
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
            print("Erro ao executar função 'atualiza_tabela_controle_lote'.", error)

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
        print("Erro ao executar função 'atualiza_tabela_controle_lote'.", result, error)

    finally:
        pgcnn.close_connection()


def insere_tabela_controle_registro_ocor(req_res):
    pgcnn = None
    if req_res is not None and len(req_res) != 0:
        try:
            pgcnn = PostgreSQLConnection()
            for item_retorno in req_res:
                sql = 'INSERT INTO public.controle_migracao_registro_ocor ' \
                      '(i_sequencial, hash_chave_dsk, sistema, tipo_registro, id_gerado, origem, situacao, resolvido,' \
                      'i_sequencial_lote, id_integracao, mensagem_erro, mensagem_ajuda, json_enviado, id_existente) ' \
                      'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cursor = pgcnn.conn.cursor()
                result = cursor.execute(sql, req_res)
                pgcnn.conn.commit()
        except Exception as error:
            print("Erro ao executar função 'atualiza_tabela_controle_lote'.", error)

        finally:
            pgcnn.close_connection()


def insere_tabela_controle_migracao_registro(params_exec, req_res):
    pgcnn = None
    if req_res is not None:
        try:
            pgcnn = PostgreSQLConnection()
            sql = 'INSERT INTO public.controle_migracao_registro ' \
                  '(sistema, tipo_registro, hash_chave_dsk, descricao_tipo_registro, id_gerado, i_chave_dsk1, ' \
                  'i_chave_dsk2, i_chave_dsk3, i_chave_dsk4, i_chave_dsk5, i_chave_dsk6, i_chave_dsk7, i_chave_dsk8,' \
                  'i_chave_dsk9, i_chave_dsk10, i_chave_dsk11, i_chave_dsk12) ' \
                  'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cursor = pgcnn.conn.cursor()

            values = (
                req_res.get('sistema'),
                req_res.get('tipo_registro'),
                req_res.get('hash_chave_dsk'),
                req_res.get('descricao_tipo_registro'),
                req_res.get('id_gerado'),
                req_res.get('i_chave_dsk1'),
                None if 'i_chave_dsk2' not in req_res else req_res.get('i_chave_dsk2'),
                None if 'i_chave_dsk3' not in req_res else req_res.get('i_chave_dsk3'),
                None if 'i_chave_dsk4' not in req_res else req_res.get('i_chave_dsk4'),
                None if 'i_chave_dsk5' not in req_res else req_res.get('i_chave_dsk5'),
                None if 'i_chave_dsk6' not in req_res else req_res.get('i_chave_dsk6'),
                None if 'i_chave_dsk7' not in req_res else req_res.get('i_chave_dsk7'),
                None if 'i_chave_dsk8' not in req_res else req_res.get('i_chave_dsk8'),
                None if 'i_chave_dsk9' not in req_res else req_res.get('i_chave_dsk9'),
                None if 'i_chave_dsk10' not in req_res else req_res.get('i_chave_dsk10'),
                None if 'i_chave_dsk11' not in req_res else req_res.get('i_chave_dsk11'),
                None if 'i_chave_dsk12' not in req_res else req_res.get('i_chave_dsk12')
            )
            result = cursor.execute(sql, values)
            pgcnn.conn.commit()

        except Exception as error:
            print("Erro ao executar função 'insere_tabela_controle_migracao_registro'.", error)

        finally:
            pgcnn.close_connection()


def valida_lotes_enviados(params_exec, *args, **kwargs):
    pgcnn = None
    retorno_analise_lote = None
    print('- Iniciando validação de lotes enviados pendentes.')

    try:
        pgcnn = PostgreSQLConnection()
        existe_pendencia = True

        while existe_pendencia:
            slq = 'SELECT id_lote, url_consulta FROM public.controle_migracao_lotes WHERE status not in (3, 4, 5)'
            if 'tipo_registro' in kwargs:
                slq += f" AND tipo_registro = '{kwargs.get('tipo_registro')}'"
            df = pgcnn.exec_sql(slq)
            existe_pendencia = False

            if len(df) == 0:
                print('- Não restam lotes pendentes para validação.')
            else:
                # Inicia rodadas de verificação de lotes pendentes
                agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                print(f'- Verificando {len(df)} lote(s) pendente(s) ({agora}).')
                headers = {'authorization': f'bearer {params_exec["token"]}', 'content-type': 'application/json'}

                # Se existir lote spendentes, iniciar o loop de consultas
                for lote in df.to_dict('records'):
                    url = lote['url_consulta']
                    req = requests.get(url, headers=headers)
                    if 'json' in req.headers.get('Content-Type'):
                        retorno_json = req.json()
                        if retorno_json['status'] in ['AGUARDANDO_EXECUCAO', 'EXECUTANDO']:
                            existe_pendencia = True
                        else:
                            retorno_analise_lote = analisa_retorno_lote(params_exec, retorno_json)
                    else:
                        print('Retorno não JSON:', req.status_code, req.text)

            if existe_pendencia:
                time.sleep(5)

        print('- Consulta de lotes finalizada.')
        if retorno_analise_lote["incosistencia_registros"] > 0:
            print(f'- {retorno_analise_lote["incosistencia_registros"]} registro(s) em '
                  f'{retorno_analise_lote["incosistencia_lotes"]} lote(s) retornaram inconsistência. '
                  f'Verificar tabela public.controle_migracao_registro_ocor para mais informações.')
        else:
            print('- Nenhuma inconsistência encontrada nos lotes enviados.')
    except Exception as error:
        print("Erro ao executar função 'atualiza_tabela_controle_lote'.", error)

    finally:
        pgcnn.close_connection()


def analisa_retorno_lote(params_exec, retorno_json, **kwargs):
    resultado_analise = {
        'incosistencia_registros': 0,
        'incosistencia_lotes': 0
    }
    status_lote = None

    try:
        # Verifica o status do lote
        if 'status' in retorno_json:
            status_lote = retorno_json['status']
        elif 'situacao' in retorno_json:
            status_lote = retorno_json['situacao']

        if status_lote in ['EXECUTADO', 'PROCESSADO']:
            status_lote = 3
        elif status_lote in ['EXECUTADO_PARCIALMENTE']:
            status_lote = 4
            resultado_analise['incosistencia_lotes'] += 1
        else:
            status_lote = 5
            resultado_analise['incosistencia_lotes'] += 1

        # Atualiza o status do lote na tabela de controle
        sql = f'UPDATE public.controle_migracao_lotes ' \
              f'SET status = {status_lote}, ' \
              f'    data_hora_ret = \'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\' ' \
              f'WHERE id_lote = \'{retorno_json["id"]}\''
        atualiza_tabela_controle_lote(sql=sql)

        # Analisa registros contidos no lote
        if 'retorno' in retorno_json:
            for registro in retorno_json['retorno']:
                if registro['status'] in ['SUCESSO', 'SUCESS', 'EXECUTADO']:
                    registro_status = 3
                    registro_resolvido = 2
                    id_gerado = registro['idGerado']
                else:
                    resultado_analise['incosistencia_registros'] += 1
                    registro_status = 1
                    registro_resolvido = 1
                    registro_mensagem = '' if 'mensagem' not in registro else registro['mensagem']
                    id_existente = None if 'idExistente' not in registro else registro['idExistente'][0]

                    # No desktop, existe uma proc chamada 'dbf_atualiza_controle_migracao_registro_integ'
                    # que faz a atualização da tabela _ocor e _registro simultaneamente, verificar
                    dados_inserir = (0, 'hash', 1, kwargs.get('tipo_registro'), None, 9, 9, 9, 9,
                                     registro['idIntegracao'], registro['mensagem'], '', '', id_existente)
                    print('dados_inserir', dados_inserir)
                    # insere_tabela_controle_registro_ocor(dados_inserir)
    except Exception as error:
        print("Erro ao executar função 'atualiza_tabela_controle_lote'.", error)
    finally:
        return resultado_analise
