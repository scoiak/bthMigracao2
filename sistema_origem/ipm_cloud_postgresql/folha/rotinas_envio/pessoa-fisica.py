import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'pessoa-fisica'
sistema = 300
limite_lote = 500
url = "https://pessoal.cloud.betha.com.br/service-layer/v1/api/pessoa-fisica"
atualizar_ja_enviados = True


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if False:
        busca_dados(params_exec)
    if True:
        # dados_assunto = coletar_dados(params_exec)
        # dados_enviar = pre_validar(params_exec, dados_assunto)
        if not params_exec.get('somente_pre_validar'):
            pass
            # iniciar_envio(params_exec, dados_enviar, 'POST')
        model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def busca_dados(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    for item in registros:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['cpf'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Pessoa Fisica',
            'id_gerado': item['id'],
            'i_chave_dsk1': item['cpf']
        })
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
    print('- Busca finalizada. Tabelas de controles atualizas com sucesso.')


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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['cpf'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'nome': None if 'nome' not in item else item['nome'],
                'cpf': None if 'cpf' not in item else item['cpf'],
                'dataNascimento': None if 'datanascimento' not in item else item['datanascimento'],
                'estadoCivil': None if 'estadocivil' not in item else item['estadocivil'],
                'sexo': None if 'sexo' not in item else item['sexo'],
                'raca': None if 'raca' not in item else item['raca'],
                'corOlhos': None if 'corolhos' not in item else item['corolhos'],
                'estatura': None if 'estatura' not in item else item['estatura'],
                'peso': None if 'peso' not in item else item['peso'],
                'tipoSanguineo': None if 'tiposanguineo' not in item else item['tiposanguineo'],
                'doador': None if 'doador' not in item else item['doador'],
                'dataChegada': None if 'datachegada' not in item else item['datachegada'],
                'casadoComBrasileiro': None if 'casadocombrasileiro' not in item else item['casadocombrasileiro'],
                'temFilhosBrasileiros': None if 'temfilhosbrasileiros' not in item else item['temfilhosbrasileiros'],
                'situacaoEstrangeiro': None if 'situacaoestrangeiro' not in item else item['situacaoestrangeiro'],
                'inscricaoMunicipal': None if 'inscricaomunicipal' not in item else item['inscricaomunicipal'],
                'identidade': None if 'identidade' not in item else item['identidade'],
                'orgaoEmissorIdentidade': None if 'orgaoemissoridentidade' not in item else item['orgaoemissoridentidade'],
                'ufEmissaoIdentidade': None if 'ufemissaoidentidade' not in item else item['ufemissaoidentidade'],
                'dataEmissaoIdentidade': None if 'dataemissaoidentidade' not in item else item['dataemissaoidentidade'],
                'dataValidadeIdentidade': None if 'datavalidadeidentidade' not in item else item['datavalidadeidentidade'],
                'tituloEleitor': None if 'tituloeleitor' not in item else item['tituloeleitor'],
                'zonaEleitoral': None if 'zonaeleitoral' not in item else item['zonaeleitoral'],
                'secaoEleitoral': None if 'secaoeleitoral' not in item else item['secaoeleitoral'],
                'ctps': None if 'ctps' not in item else item['ctps'],
                'serieCtps': None if 'seriectps' not in item else item['seriectps'],
                'ufEmissaoCtps': None if 'ufEmissaoctps' not in item else item['ufEmissaoctps'],
                'dataEmissaoCtps': None if 'dataemissaoctps' not in item else item['dataemissaoctps'],
                'dataValidadeCtps': None if 'datavalidadectps' not in item else item['datavalidadectps'],
                'pis': None if 'pis' not in item else item['pis'],
                'dataEmissaoPis': None if 'dataemissaopis' not in item else item['dataemissaopis'],
                'situacaoGrauInstrucao': None if 'situacaograuinstrucao' not in item else item['situacaograuinstrucao'],
                'grauInstrucao': None if 'grauinstrucao' not in item else item['grauinstrucao'],
                'certificadoReservista': None if 'certificadoreservista' not in item else item['certificadoreservista'],
                'ric': None if 'ric' not in item else item['ric'],
                'ufEmissaoRic': None if 'ufemissaoric' not in item else item['ufemissaoric'],
                'orgaoEmissorRic': None if 'orgaoemissorric' not in item else item['orgaoemissorric'],
                'dataEmissaoRic': None if 'dataemissaoric' not in item else item['dataemissaoric'],
                'cns': None if 'cns' not in item else item['cns'],
                'dataEmissaoCns': None if 'dataemissaocns' not in item else item['dataemissaocns'],
                'cnh': None if 'cnh' not in item else item['cnh'],
                'categoriaCnh': None if 'categoriacnh' not in item else item['categoriacnh'],
                'dataEmissaoCnh': None if 'dataemissaocnh' not in item else item['dataemissaocnh'],
                'dataVencimentoCnh': None if 'datavencimentocnh' not in item else item['datavencimentocnh'],
                'dataPrimeiraCnh': None if 'dataprimeiracnh' not in item else item['dataprimeiracnh'],
                'ufEmissaoCnh': None if 'ufemissaocnh' not in item else item['ufemissaocnh'],
                'observacoesCnh': None if 'observacoescnh' not in item else item['observacoescnh'],
                'papel': None if 'papel' not in item else item['papel']
            }
        }
        if item['emails'] is not None:
            dict_dados['conteudo'].update({
                'emails': []
            })
            lista = item['emails'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['emails'].append({
                    'descricao': campo[0],
                    'endereco': campo[1],
                    'principal': campo[2]
                })
        if item['telefones'] is not None:
            dict_dados['conteudo'].update({
                'telefones': []
            })
            lista = item['telefones'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['telefones'].append({
                    'descricao': campo[0],
                    'tipo': campo[1],
                    'numero': campo[2],
                    'observacao': campo[3],
                    'principal': campo[4]
                })
        if item['enderecos'] is not None:
            dict_dados['conteudo'].update({
                'enderecos': []
            })
            lista = item['enderecos'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['enderecos'].append({
                    'descricao': campo[0],
                    'logradouro': {
                        'id': campo[1]
                    },
                    'bairro': {
                        'id': campo[2]
                    },
                    'cep': campo[3],
                    'numero': campo[4],
                    'complemento': campo[5],
                    'principal': campo[6]
                })
        if item['contasbancarias'] is not None:
            dict_dados['conteudo'].update({
                'contasBancarias': []
            })
            lista = item['contasbancarias'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['contasBancarias'].append({
                    'agencia': {
                        'id': campo[1]
                    },
                    'numero': campo[2],
                    'digito': campo[3],
                    'tipo': campo[4],
                    'dataAbertura': campo[5],
                    'dataFechamento': campo[6],
                    'situacao': campo[7],
                    'principal': campo[8]
                })
        if item['filiacoes'] is not None:
            dict_dados['conteudo'].update({
                'filiacoes': []
            })
            lista = item['filiacoes'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['filiacoes'].append({
                    'nome': campo[0],
                    'tipoFiliacao': campo[1],
                    'naturezaFiliacao': campo[2]
                })
        if item['deficiencias'] is not None:
            dict_dados['conteudo'].update({
                'deficiencias': []
            })
            dict_dados['conteudo']['deficiencias'].append({
                'tipo': item['deficiencias']
            })
        if item['nacionalidade'] is not None:
            dict_dados['conteudo'].update({
                'nacionalidade': {
                    'id': item['nacionalidade']
                }
            })
        if item['paisnascimento'] is not None:
            dict_dados['conteudo'].update({
                'paisNascimento': {
                    'id': item['paisnascimento']
                }
            })
        if item['naturalidade'] is not None:
            dict_dados['conteudo'].update({
                'naturalidade': {
                    'id': item['naturalidade']
                }
            })
        if item['naturalizado'] is not None:
            dict_dados['conteudo'].update({
                'naturalizado': item['naturalizado']
            })
        if item['observacoes'] is not None:
            dict_dados['conteudo'].update({
                'camposAdicionais': {
                    "tipo": "PESSOA_FISICA",
                    'campos': [
                        {
                            'id': item['id_ca_observacoes'],
                            'valor': item['observacoes']
                        }
                    ]
                }
            })
        if atualizar_ja_enviados and item['id_gerado'] != 0:
            dict_dados['conteudo'].update({
                'id': item['id_gerado']
            })
        contador += 1
        print(f'Dados gerados ({contador}): ', json.dumps(dict_dados))
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Pessoa Fisica',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['cpf']
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