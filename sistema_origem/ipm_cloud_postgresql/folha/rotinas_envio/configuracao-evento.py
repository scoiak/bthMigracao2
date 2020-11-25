import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'configuracao-evento'
sistema = 300
limite_lote = 500
url = "https://pessoal.cloud.betha.com.br/service-layer/v1/api/configuracao-evento"


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
        query = model.get_consulta(params_exec, tipo_registro + '.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
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
    contador = 0
    for item in dados:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['id_entidade'], item['codigo'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'codigo': None if 'codigo' not in item else item['codigo'],
                'descricao': None if 'descricao' not in item else item['descricao'],
                'inicioVigencia': None if 'iniciovigencia' not in item else item['iniciovigencia'],
                'tipo': None if 'tipo' not in item else item['tipo'],
                'classificacao': None if 'classificacao' not in item else item['classificacao'],
                'classificacaoBaixaProvisao': None if 'classificacaobaixaprovisao' not in item else item['classificacaobaixaprovisao'],
                'unidade': None if 'unidade' not in item else item['unidade'],
                'taxa': None if 'taxa' not in item else item['taxa'],
                'codigoEsocial': None if 'codigoesocial' not in item else item['codigoesocial'],
                'incideDsr': None if 'incidedsr' not in item else item['incidedsr'],
                'naturezaRubrica': None if 'naturezarubrica' not in item else item['naturezarubrica'],
                'compoemHorasMes': None if 'compoemhorasmes' not in item else item['compoemhorasmes'],
                'observacao': None if 'observacao' not in item else item['observacao'],
                'desabilitado': None if 'desabilitado' not in item else item['desabilitado'],
                'formula': None if 'formula' not in item else item['formula'],
                'enviaTransparencia': None if 'enviatransparencia' not in item else item['enviatransparencia'],
                'configuracaoProcessamentos': None if 'configuracaoprocessamentos' not in item else item['configuracaoprocessamentos'],
            }
        }
        if 'ato' in item and item['ato'] is not None:
            dict_dados['conteudo'].update({
                'ato': {
                    'id': int(item['ato'])
                }})
        contador += 1
        print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Evento',
            'id_gerado': None,
            'i_chave_dsk1': item['id_entidade'],
            'i_chave_dsk2': item['codigo']
        })
    if True:
        model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')