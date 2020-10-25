import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'pessoa-fisica'
sistema = 300
limite_lote = 300
url = "https://pessoal.cloud.betha.com.br/service-layer/v1/api/pessoa-fisica"

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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['codigo'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'nome': None if 'nome' not in item else item['nome'],
                'cpf': None if 'cpf' not in item else item['cpf'],
                'dataNascimento': None if 'dataNascimento' not in item else item['dataNascimento'],
                'estadoCivil': None if 'estadoCivil' not in item else item['estadoCivil'],
                'sexo': None if 'sexo' not in item else item['sexo'],
                'raca': None if 'raca' not in item else item['raca'],
                'corOlhos': None if 'corOlhos' not in item else item['corOlhos'],
                'estatura': None if 'estatura' not in item else item['estatura'],
                'peso': None if 'peso' not in item else item['peso'],
                'tipoSanguineo': None if 'tipoSanguineo' not in item else item['tipoSanguineo'],
                'doador': None if 'doador' not in item else item['doador'],
                #'nacionalidade': None if 'nacionalidade' not in item else item['nacionalidade'],
                'paisNascimento': None if 'paisNascimento' not in item else item['paisNascimento'],
                #'naturalidade': None if 'naturalidade' not in item else item['naturalidade'],
                #'naturalizado': None if 'naturalizado' not in item else item['naturalizado'],
                'dataChegada': None if 'dataChegada' not in item else item['dataChegada'],
                'casadoComBrasileiro': None if 'casadoComBrasileiro' not in item else item['casadoComBrasileiro'],
                'temFilhosBrasileiros': None if 'temFilhosBrasileiros' not in item else item['temFilhosBrasileiros'],
                'situacaoEstrangeiro': None if 'situacaoEstrangeiro' not in item else item['situacaoEstrangeiro'],
                'inscricaoMunicipal': None if 'inscricaoMunicipal' not in item else item['inscricaoMunicipal'],
                'identidade': None if 'identidade' not in item else item['identidade'],
                'orgaoEmissorIdentidade': None if 'orgaoEmissorIdentidade' not in item else item['orgaoEmissorIdentidade'],
                'ufEmissaoIdentidade': None if 'ufEmissaoIdentidade' not in item else item['ufEmissaoIdentidade'],
                'dataEmissaoIdentidade': None if 'dataEmissaoIdentidade' not in item else item['dataEmissaoIdentidade'],
                'dataValidadeIdentidade': None if 'dataValidadeIdentidade' not in item else item['dataValidadeIdentidade'],
                'tituloEleitor': None if 'tituloEleitor' not in item else item['tituloEleitor'],
                'zonaEleitoral': None if 'zonaEleitoral' not in item else item['zonaEleitoral'],
                'secaoEleitoral': None if 'secaoEleitoral' not in item else item['secaoEleitoral'],
                'ctps': None if 'ctps' not in item else item['ctps'],
                'serieCtps': None if 'serieCtps' not in item else item['serieCtps'],
                'ufEmissaoCtps': None if 'ufEmissaoCtps' not in item else item['ufEmissaoCtps'],
                'dataEmissaoCtps': None if 'dataEmissaoCtps' not in item else item['dataEmissaoCtps'],
                'dataValidadeCtps': None if 'dataValidadeCtps' not in item else item['dataValidadeCtps'],
                'pis': None if 'pis' not in item else item['pis'],
                'dataEmissaoPis': None if 'dataEmissaoPis' not in item else item['dataEmissaoPis'],
                'situacaoGrauInstrucao': None if 'situacaoGrauInstrucao' not in item else item['situacaoGrauInstrucao'],
                'grauInstrucao': None if 'grauInstrucao' not in item else item['grauInstrucao'],
                'certificadoReservista': None if 'certificadoReservista' not in item else item['certificadoReservista'],
                'ric': None if 'ric' not in item else item['ric'],
                'ufEmissaoRic': None if 'ufEmissaoRic' not in item else item['ufEmissaoRic'],
                'orgaoEmissorRic': None if 'orgaoEmissorRic' not in item else item['orgaoEmissorRic'],
                'dataEmissaoRic': None if 'dataEmissaoRic' not in item else item['dataEmissaoRic'],
                'cns': None if 'cns' not in item else item['cns'],
                'dataEmissaoCns': None if 'dataEmissaoCns' not in item else item['dataEmissaoCns'],
                'cnh': None if 'cnh' not in item else item['cnh'],
                'categoriaCnh': None if 'categoriaCnh' not in item else item['categoriaCnh'],
                'dataEmissaoCnh': None if 'dataEmissaoCnh' not in item else item['dataEmissaoCnh'],
                'dataVencimentoCnh': None if 'dataVencimentoCnh' not in item else item['dataVencimentoCnh'],
                'dataPrimeiraCnh': None if 'dataPrimeiraCnh' not in item else item['dataPrimeiraCnh'],
                'ufEmissaoCnh': None if 'ufEmissaoCnh' not in item else item['ufEmissaoCnh'],
                'observacoesCnh': None if 'observacoesCnh' not in item else item['observacoesCnh'],
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
                dict_dados['conteudo']['contasbancarias'].append({
                    'agencia': {
                        'id': campo[1]
                    },
                    'numero': campo[3],
                    'digito': campo[4],
                    'tipo': campo[5],
                    'dataAbertura': campo[6],
                    'dataFechamento': campo[7],
                    'situacao': campo[8],
                    'principal': campo[9]
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
                })
        if item['deficiencias'] is not None:
            dict_dados['conteudo'].update({
                'deficiencias': []
            })
            dict_dados['conteudo']['deficiencias'].append({
                'tipo': item['deficiencias']
            })
        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Pessoa Física',
            'id_gerado': None,
            'i_chave_dsk1': item['codigo']
        })
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
    req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')