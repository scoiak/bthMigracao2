import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'matricula'
sistema = 300
limite_lote = 250
url = "https://pessoal.cloud.betha.com.br/service-layer/v1/api/matricula"


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if True:
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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['codigoMatricula'], item['numeroContrato'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Matricula',
            'id_gerado': item['id'],
            'i_chave_dsk1': item['codigoMatricula'],
            'i_chave_dsk2': item['numeroContrato'],
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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['codigomatricula'], item['numerocontrato'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'vinculoEmpregaticio': {
                    'id': item['vinculoempregaticio']
                },
                #'contratoTemporario': None if 'contratotemporario' not in item else item['contratotemporario'],
                #'previdencias': None if 'previdencias' not in item else item['previdencias'],
                'cargo': {
                    'id': item['cargo']
                },
                #'funcoesGratificadas	': None if 'funcoesGratificadas	' not in item else item['funcoesGratificadas	'],
                #'responsaveis': None if 'responsaveis' not in item else item['responsaveis'],
                #'codigoMatricula': None if 'codigoMatricula' not in item else item['codigoMatricula'],
                #'validationStatus': None if 'validationstatus' not in item else item['validationstatus'],
                #'lotacoesFisicas': None if 'lotacoesfisicas' not in item else item['lotacoesfisicas'],
                #'historicos': None if 'historicos' not in item else item['historicos']
            }
        }
        if 'database' in item and item['database'] is not None:
            dict_dados['conteudo'].update({'dataBase': item['database']})
        if 'numeroapolicesegurovida' in item and item['numeroapolicesegurovida'] is not None:
            dict_dados['conteudo'].update({'numeroApoliceSeguroVida': item['numeroapolicesegurovida']})
        if 'possuisegurovida' in item and item['possuisegurovida'] is not None:
            dict_dados['conteudo'].update({'possuiSeguroVida': item['possuisegurovida']})
        if 'origemsalario' in item and item['origemsalario'] is not None:
            dict_dados['conteudo'].update({'origemSalario': item['origemsalario']})
        if 'salarioalterado' in item and item['salarioalterado'] is not None:
            dict_dados['conteudo'].update({'salarioAlterado': item['salarioalterado']})
        if 'indicativoadmissao' in item and item['indicativoadmissao'] is not None:
            dict_dados['conteudo'].update({'indicativoAdmissao': item['indicativoadmissao']})
        if 'rendimentomensal' in item and item['rendimentomensal'] is not None:
            dict_dados['conteudo'].update({'rendimentoMensal': item['rendimentomensal']})
        if 'naturezaatividade' in item and item['naturezaatividade'] is not None:
            dict_dados['conteudo'].update({'naturezaAtividade': item['naturezaatividade']})
        if 'tipoadmissao' in item and item['tipoadmissao'] is not None:
            dict_dados['conteudo'].update({'tipoAdmissao': item['tipoadmissao']})
        if 'primeiroemprego' in item and item['primeiroemprego'] is not None:
            dict_dados['conteudo'].update({'primeiroEmprego': item['primeiroemprego']})
        if 'tipoprovimento' in item and item['tipoprovimento'] is not None:
            dict_dados['conteudo'].update({'tipoProvimento': item['tipoprovimento']})
        if 'datanomeacao' in item and item['datanomeacao'] is not None:
            dict_dados['conteudo'].update({'dataNomeacao': item['datanomeacao']})
        if 'dataposse' in item and item['dataposse'] is not None:
            dict_dados['conteudo'].update({'dataPosse': item['dataposse']})
        if 'tempoaposentadoria' in item and item['tempoaposentadoria'] is not None:
            dict_dados['conteudo'].update({'tempoAposentadoria': item['tempoaposentadoria']})
        if 'previdenciafederal' in item and item['previdenciafederal'] is not None:
            dict_dados['conteudo'].update({'previdenciaFederal': item['previdenciafederal']})
        if 'cargoalterado' in item and item['cargoalterado'] is not None:
            dict_dados['conteudo'].update({'cargoAlterado': item['cargoalterado']})
        if 'areaatuacaoalterada' in item and item['areaatuacaoalterada'] is not None:
            dict_dados['conteudo'].update({'areaAtuacaoAlterada': item['areaatuacaoalterada']})
        if 'ocupavaga' in item and item['ocupavaga'] is not None:
            dict_dados['conteudo'].update({'ocupaVaga': item['ocupavaga']})
        if 'ocupavagacomissionado' in item and item['ocupavagacomissionado'] is not None:
            dict_dados['conteudo'].update({'ocupaVagaComissionado': item['ocupavagacomissionado']})
        if 'salariocomissionado' in item and item['salariocomissionado'] is not None:
            dict_dados['conteudo'].update({'salarioComissionado': item['salariocomissionado']})
        if 'unidadepagamento' in item and item['unidadepagamento'] is not None:
            dict_dados['conteudo'].update({'unidadePagamento': item['unidadepagamento']})
        if 'formapagamento' in item and item['formapagamento'] is not None:
            dict_dados['conteudo'].update({'formaPagamento': item['formapagamento']})
        if 'quantidadehorasmes' in item and item['quantidadehorasmes'] is not None:
            dict_dados['conteudo'].update({'quantidadeHorasMes': item['quantidadehorasmes']})
        if 'quantidadehorassemana' in item and item['quantidadehorassemana'] is not None:
            dict_dados['conteudo'].update({'quantidadeHorasSemana': item['quantidadehorassemana']})
        if 'jornadaparcial' in item and item['jornadaparcial'] is not None:
            dict_dados['conteudo'].update({'jornadaParcial': item['jornadaparcial']})
        if 'dataagendamentorescisao' in item and item['dataagendamentorescisao'] is not None:
            dict_dados['conteudo'].update({'dataAgendamentoRescisao': item['dataagendamentorescisao']})
        if 'dataterminocontratotemporario' in item and item['dataterminocontratotemporario'] is not None:
            dict_dados['conteudo'].update({'dataTerminoContratoTemporario': item['dataterminocontratotemporario']})
        if 'motivocontratotemporario' in item and item['motivocontratotemporario'] is not None:
            dict_dados['conteudo'].update({'motivoContratoTemporario': item['motivocontratotemporario']})
        if 'tipoinclusaocontratotemporario' in item and item['tipoinclusaocontratotemporario'] is not None:
            dict_dados['conteudo'].update({'tipoInclusaoContratoTemporario': item['tipoinclusaocontratotemporario']})
        if 'dataprorrogacaocontratotemporario' in item and item['dataprorrogacaocontratotemporario'] is not None:
            dict_dados['conteudo'].update({'dataProrrogacaoContratoTemporario': item['dataprorrogacaocontratotemporario']})
        if 'numerocartaoponto' in item and item['numerocartaoponto'] is not None:
            dict_dados['conteudo'].update({'numeroCartaoPonto': item['numerocartaoponto']})
        if 'indicativoprovimento' in item and item['indicativoprovimento'] is not None:
            dict_dados['conteudo'].update({'indicativoProvimento': item['indicativoprovimento']})
        if 'matriculaempresaorigem' in item and item['matriculaempresaorigem'] is not None:
            dict_dados['conteudo'].update({'matriculaEmpresaOrigem': item['matriculaempresaorigem']})
        if 'dataadmissaoorigem' in item and item['dataadmissaoorigem'] is not None:
            dict_dados['conteudo'].update({'dataAdmissaoOrigem': item['dataadmissaoorigem']})
        if 'ocorrenciasefip' in item and item['ocorrenciasefip'] is not None:
            dict_dados['conteudo'].update({'ocorrenciaSefip': item['ocorrenciasefip']})
        if 'controlejornada' in item and item['controlejornada'] is not None:
            dict_dados['conteudo'].update({'controleJornada': item['controlejornada']})
        if 'processaaverbacao' in item and item['processaaverbacao'] is not None:
            dict_dados['conteudo'].update({'processaAverbacao': item['processaaverbacao']})
        if 'datafinal' in item and item['datafinal'] is not None:
            dict_dados['conteudo'].update({'dataFinal': item['datafinal']})
        if 'dataprorrogacao' in item and item['dataprorrogacao'] is not None:
            dict_dados['conteudo'].update({'dataProrrogacao': item['dataprorrogacao']})
        if 'formacaoperiodo' in item and item['formacaoperiodo'] is not None:
            dict_dados['conteudo'].update({'formacaoPeriodo': item['formacaoperiodo']})
        if 'formacaofase' in item and item['formacaofase'] is not None:
            dict_dados['conteudo'].update({'formacaoFase': item['formacaofase']})
        if 'estagioobrigatorio' in item and item['estagioobrigatorio'] is not None:
            dict_dados['conteudo'].update({'estagioObrigatorio': item['estagioobrigatorio']})
        if 'objetivo' in item and item['objetivo'] is not None:
            dict_dados['conteudo'].update({'objetivo': item['objetivo']})
        if 'numerocontrato' in item and item['numerocontrato'] is not None:
            dict_dados['conteudo'].update({'numeroContrato': item['numerocontrato']})
        if 'datacessacaoaposentadoria' in item and item['datacessacaoaposentadoria'] is not None:
            dict_dados['conteudo'].update({'dataCessacaoAposentadoria': item['datacessacaoaposentadoria']})
        if 'motivoiniciobeneficio' in item and item['motivoiniciobeneficio'] is not None:
            dict_dados['conteudo'].update({'motivoInicioBeneficio': item['motivoiniciobeneficio']})
        if 'duracaobeneficio' in item and item['duracaobeneficio'] is not None:
            dict_dados['conteudo'].update({'duracaoBeneficio': item['duracaobeneficio']})
        if 'datacessacaobeneficio' in item and item['datacessacaobeneficio'] is not None:
            dict_dados['conteudo'].update({'dataCessacaoBeneficio': item['datacessacaobeneficio']})
        if 'datainiciocontrato' in item and item['datainiciocontrato'] is not None:
            dict_dados['conteudo'].update({'dataInicioContrato': item['datainiciocontrato']})
        if 'situacao' in item and item['situacao'] is not None:
            dict_dados['conteudo'].update({'situacao': item['situacao']})
        if 'iniciovigencia' in item and item['iniciovigencia'] is not None:
            dict_dados['conteudo'].update({'inicioVigencia': item['iniciovigencia']})
        if 'tipo' in item and item['tipo'] is not None:
            dict_dados['conteudo'].update({'tipo': item['tipo']})
        if 'esocial' in item and item['esocial'] is not None:
            dict_dados['conteudo'].update({'eSocial': item['esocial']})
        if item['codigomatricula'] is not None:
            campo = item['codigomatricula'].split('%|%')
            dict_dados['conteudo'].update({
                'codigoMatricula': {
                    'contrato': int(campo[0]),
                    'digitoVerificador': int(campo[1]),
                    'numero': int(campo[2])
                }
            })
        if False:
            if 'sindicato' in item and item['sindicato'] is not None:
                dict_dados['conteudo'].update({
                    'sindicato': {
                        'id': item['sindicato']
                    }
                })
        if 'leicontrato' in item and item['leicontrato'] is not None:
            dict_dados['conteudo'].update({
                'leiContrato': {
                    'id': item['leicontrato']
                }
            })
        if 'atocontrato' in item and item['atocontrato'] is not None:
            dict_dados['conteudo'].update({
                'atoContrato': {
                    'id': item['atocontrato']
                }
            })
        if 'atoalteracaocargo' in item and item['atoalteracaocargo'] is not None:
            dict_dados['conteudo'].update({
                'atoAlteracaoCargo': {
                    'id': item['atoalteracaocargo']
                }
            })
        if 'areaatuacao' in item and item['areaatuacao'] is not None:
            dict_dados['conteudo'].update({
                'areaAtuacao': {
                    'id': item['areaatuacao']
                }
            })
        if 'motivoalteracaoareaatuacao' in item and item['motivoalteracaoareaatuacao'] is not None:
            dict_dados['conteudo'].update({
                'motivoAlteracaoAreaAtuacao': {
                    'id': item['motivoalteracaoareaatuacao']
                }
            })
        if 'nivelsalarial' in item and item['nivelsalarial'] is not None:
            dict_dados['conteudo'].update({
                'nivelSalarial': {
                    'id': item['nivelsalarial']
                }
            })
        if 'classereferencia' in item and item['classereferencia'] is not None:
            dict_dados['conteudo'].update({
                'classeReferencia': {
                    'id': item['classereferencia']
                }
            })
        if 'cargocomissionado' in item and item['cargocomissionado'] is not None:
            dict_dados['conteudo'].update({
                'cargoComissionado': {
                    'id': item['cargocomissionado']
                }
            })
        if 'areaatuacaocomissionado' in item and item['areaatuacaocomissionado'] is not None:
            dict_dados['conteudo'].update({
                'areaAtuacaoComissionado': {
                    'id': item['areaatuacaocomissionado']
                }
            })
        if 'nivelsalarialcomissionado' in item and item['nivelsalarialcomissionado'] is not None:
            dict_dados['conteudo'].update({
                'nivelSalarialComissionado': {
                    'id': item['nivelsalarialcomissionado']
                }
            })
        if 'classereferenciacomissionado' in item and item['classereferenciacomissionado'] is not None:
            dict_dados['conteudo'].update({
                'classeReferenciaComissionado': {
                    'id': item['classereferenciacomissionado']
                }
            })
        if False:
            if 'contabancariapagamento' in item and item['contabancariapagamento'] is not None:
                dict_dados['conteudo'].update({
                    'contaBancariaPagamento': {
                        'id': item['contabancariapagamento']
                    }
                })
        if 'configuracaoferias' in item and item['configuracaoferias'] is not None:
            dict_dados['conteudo'].update({
                'configuracaoFerias': {
                    'id': item['configuracaoferias']
                }
            })
        if 'parametroponto' in item and item['parametroponto'] is not None:
            dict_dados['conteudo'].update({
                'parametroPonto': {
                    'id': item['parametroponto']
                }
            })
        if 'orgaoorigem' in item and item['orgaoorigem'] is not None:
            dict_dados['conteudo'].update({
                'orgaoOrigem': {
                    'id': item['orgaoorigem']
                }
            })
        if 'configuracaolicencapremio' in item and item['configuracaolicencapremio'] is not None:
            dict_dados['conteudo'].update({
                'configuracaoLicencaPremio': {
                    'id': item['configuracaolicencapremio']
                }
            })
        if 'configuracaoadicional' in item and item['configuracaoadicional'] is not None:
            dict_dados['conteudo'].update({
                'configuracaoAdicional': {
                    'id': item['configuracaoadicional']
                }
            })
        if 'instituicaoensino' in item and item['instituicaoensino'] is not None:
            dict_dados['conteudo'].update({
                'instituicaoEnsino': {
                    'id': item['instituicaoensino']
                }
            })
        if 'agenteintegracao' in item and item['agenteintegracao'] is not None:
            dict_dados['conteudo'].update({
                'agenteIntegracao': {
                    'id': item['agenteintegracao']
                }
            })
        if 'formacao' in item and item['formacao'] is not None:
            dict_dados['conteudo'].update({
                'formacao': {
                    'id': item['formacao']
                }
            })
        if 'categoriatrabalhador' in item and item['categoriatrabalhador'] is not None:
            dict_dados['conteudo'].update({
                'categoriaTrabalhador': {
                    'id': item['categoriatrabalhador']
                }
            })
        if 'motivoaposentadoria' in item and item['motivoaposentadoria'] is not None:
            dict_dados['conteudo'].update({
                'motivoAposentadoria': {
                    'id': item['motivoaposentadoria']
                }
            })
        if 'funcionarioorigem' in item and item['funcionarioorigem'] is not None:
            dict_dados['conteudo'].update({
                'funcionarioOrigem': {
                    'id': item['funcionarioorigem']
                }
            })
        if 'tipomovimentacao' in item and item['tipomovimentacao'] is not None:
            dict_dados['conteudo'].update({
                'tipoMovimentacao': {
                    'id': item['tipomovimentacao']
                }
            })
        if 'matriculaorigem' in item and item['matriculaorigem'] is not None:
            dict_dados['conteudo'].update({
                'matriculaOrigem': {
                    'id': item['matriculaorigem']
                }
            })
        if 'responsavel' in item and item['responsavel'] is not None:
            dict_dados['conteudo'].update({
                'responsavel': {
                    'id': item['responsavel']
                }
            })
        if 'pessoa' in item and item['pessoa'] is not None:
            dict_dados['conteudo'].update({
                'pessoa': {
                    'id': item['pessoa']
                }
            })
        if 'grupofuncional' in item and item['grupofuncional'] is not None:
            dict_dados['conteudo'].update({
                'grupoFuncional': {
                    'id': item['grupofuncional']
                }
            })
        if 'jornadatrabalho' in item and item['jornadatrabalho'] is not None:
            dict_dados['conteudo'].update({
                'jornadaTrabalho': {
                    'id': item['jornadatrabalho']
                }
            })
        if 'atoalteracaosalario' in item and item['atoalteracaosalario'] is not None:
            dict_dados['conteudo'].update({
                'atoAlteracaoSalario': {
                    'id': item['atoalteracaosalario']
                }
            })
        if 'motivoalteracaosalario' in item and item['motivoalteracaosalario'] is not None:
            dict_dados['conteudo'].update({
                'motivoAlteracaoSalario': {
                    'id': item['motivoalteracaosalario']
                }
            })
        if 'organograma' in item and item['organograma'] is not None:
            dict_dados['conteudo'].update({
                'organograma': {
                    'id': item['organograma']
                }
            })
        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Matricula',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['codigomatricula'],
            'i_chave_dsk2': item['numerocontrato'],
        })
    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
    if False:
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)
        print('- Envio de dados finalizado.')
