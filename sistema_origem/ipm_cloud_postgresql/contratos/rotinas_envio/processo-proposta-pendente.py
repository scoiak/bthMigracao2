import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'processo-proposta-pendente'
url = 'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoAdministrativoId}/proposta-participante'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # T - Realiza a pré-validação dos dados
    dados_enviar = pre_validar(params_exec, dados_assunto)

    # L - Realiza o envio dos dados validados
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')


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
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    total_dados = len(dados)

    for reg in dados:
        query = f'''select
                    row_number() over() as id,
                    '305' as sistema,
                    'processo-proposta-pendente' as tipo_registro,
                    *
                from (
                    select distinct
                        aux.*,
                        (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_participante,
                        0.00 as valor_unitario,
                        i.cmiqtde as quantidade,
                        null as marca,
                        'NAO_COTOU' as situacao,
                        0 as colocacao,
                        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', aux.clicodigo, aux.minano, aux.minnro))) as id_processo,
                        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', aux.clicodigo, aux.minano, aux.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_participante,
                        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', aux.clicodigo, aux.minano, aux.minnro, '@', coalesce(i.lotcodigo, 0) , '@', i.cmiitem))) as id_item,
                        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-proposta-pendente', aux.clicodigo, aux.minano, aux.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), aux.cmiid))) as id_gerado
                    from (
                        select
                            participante.clicodigo,
                            participante.minano,
                            participante.minnro,
                            item.cmiid ,
                            participante.unicodigo
                        from (
                            select pl.clicodigo, pl.minano, pl.minnro, pl.unicodigo
                            from wco.tbparlic pl
                            where pl.clicodigo = {reg['clicodigo']}
                            and pl.minano = {reg['minano']}
                            and pl.minnro = {reg['minnro']}
                        ) participante
                        cross join (
                            select it.clicodigo, it.minano, it.minnro, it.cmiid
                            from wco.tbitemin it
                            where it.clicodigo = {reg['clicodigo']}
                            and it.minano = {reg['minano']}
                            and it.minnro = {reg['minnro']}
                        ) item
                    ) aux
                    left join wco.tbitemin i on (i.clicodigo = aux.clicodigo and i.minano = aux.minano and i.minnro = aux.minnro and i.cmiid = aux.cmiid)
                    inner join wun.tbunico u on (u.unicodigo = aux.unicodigo)
                    where not exists (
                        select 1
                        from wco.tbcadqcp qcp
                        where qcp.clicodigo = aux.clicodigo
                        and qcp.minano = aux.minano
                        and qcp.minnro = aux.minnro
                        and qcp.unicodigo = aux.unicodigo
                        and qcp.cmiid = aux.cmiid
                    )
                    and exists (
                        select 1
                        from wco.tbcadqcp qcp
                        where qcp.clicodigo = aux.clicodigo
                        and qcp.minano = aux.minano
                        and qcp.minnro = aux.minnro
                    )
                ) tab
                where id_gerado is null
                and id_processo is not null
                and id_participante is not null
                and id_item is not null
                order by 1, 2 desc, 3 desc, 4 asc'''
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        lista_dados = [i for i in df.to_dict('records')]
        total_erros = 0
        contador = 0
        total_dados = len(lista_dados)
        if total_dados > 0:
            print(f'- Enviando propostas pendentes do processo {reg["minnro"]}/{reg["minano"]} ({reg["clicodigo"]})')
        for item in lista_dados:
            lista_dados_enviar = []
            lista_controle_migracao = []
            contador += 1
            print(f'\r- Enviando registros: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['clicodigo'], item['minano'],
                                                  item['minnro'], item['cpf_participante'], item['cmiid'])
            url_parametrizada = url.replace('{exercicio}', str(item['minano'])) \
                .replace('{processoAdministrativoId}', str(item['id_processo']))
            dict_dados = {
                'idIntegracao': hash_chaves,
                'url': url_parametrizada,
                'processoAdministrativo': {
                    'id': item['id_processo']
                },
                'participante': {
                    'id': item['id_participante']
                },
                'item': {
                    'id': item['id_item']
                },
                'situacao': {
                    'valor': item['situacao']
                },
                'quantidade': item['quantidade'],
                'valorUnitarioPercentual': item['valor_unitario']
            }

            if item['marca'] is not None:
                dict_dados.update({'marca': item['marca']})

            # print(f'Dados gerados ({contador}): ', dict_dados)
            lista_dados_enviar.append(dict_dados)
            lista_controle_migracao.append({
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro de Propostas Pendentes do Processo',
                'id_gerado': None,
                'json': json.dumps(dict_dados),
                'i_chave_dsk1': item['clicodigo'],
                'i_chave_dsk2': item['minano'],
                'i_chave_dsk3': item['minnro'],
                'i_chave_dsk4': item['cpf_participante'],
                'i_chave_dsk5': item['cmiid']
            })

            if True:
                model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
                req_res = interacao_cloud \
                    .preparar_requisicao_sem_lote(
                        lista_dados=lista_dados_enviar,
                        token=token,
                        url=url,
                        tipo_registro=tipo_registro)
                model.atualiza_tabelas_controle_envio_sem_lote(params_exec, req_res, tipo_registro=tipo_registro)
                if req_res[0]['mensagem'] is not None:
                    total_erros += 1
        if total_dados > 0:
            if total_erros > 0:
                print(f'- Envio finalizado. Foram encontrados um total de {total_erros} inconsistência(s) de envio.')
            else:
                print('- Envio de dados finalizado sem inconsistências.')
