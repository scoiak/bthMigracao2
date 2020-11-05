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
                'indicativoAdmissao': None if 'indicativoadmissao' not in item else item['indicativoadmissao'],
                'naturezaAtividade': None if 'naturezaatividade' not in item else item['naturezaatividade'],
                'tipoAdmissao': None if 'tipoadmissao' not in item else item['tipoadmissao'],
                'primeiroEmprego': None if 'primeiroemprego' not in item else item['primeiroemprego'],
                'optanteFgts': None if 'optantefgts' not in item else item['optantefgts'],
                'dataOpcao': None if 'dataopcao' not in item else item['dataopcao'],
                'contaFgts': None if 'contafgts' not in item else item['contafgts'],
                'tipoProvimento': None if 'tipoprovimento' not in item else item['tipoprovimento'],
                'dataNomeacao': None if 'datanomeacao' not in item else item['datanomeacao'],
                'dataPosse': None if 'dataposse' not in item else item['dataposse'],
                'tempoAposentadoria': None if 'tempoaposentadoria' not in item else item['tempoaposentadoria'],
                'previdenciaFederal': None if 'previdenciafederal' not in item else item['previdenciafederal'],
                #'previdencias': None if 'previdencias' not in item else item['previdencias'],
                'cargo': {
                    'id': item['cargo']
                },
                'cargoAlterado': None if 'cargoalterado' not in item else item['cargoalterado'],
                'areaAtuacaoAlterada': None if 'areaatuacaoalterada' not in item else item['areaatuacaoalterada'],
                'ocupaVaga': None if 'ocupavaga' not in item else item['ocupavaga'],
                'salarioAlterado': None if 'salarioalterado' not in item else item['salarioalterado'],
                'origemSalario': None if 'origemsalario' not in item else item['origemsalario'],
                'ocupaVagaComissionado': None if 'ocupavagacomissionado' not in item else item['ocupavagacomissionado'],
                'salarioComissionado': None if 'salariocomissionado' not in item else item['salariocomissionado'],
                'unidadePagamento': None if 'unidadepagamento' not in item else item['unidadepagamento'],
                'formaPagamento': None if 'formapagamento' not in item else item['formapagamento'],
                'quantidadeHorasMes': None if 'quantidadehorasmes' not in item else item['quantidadehorasmes'],
                'quantidadeHorasSemana': None if 'quantidadehorassemana' not in item else item['quantidadehorassemana'],
                'jornadaParcial': None if 'jornadaparcial' not in item else item['jornadaparcial'],
                'dataAgendamentoRescisao': None if 'dataagendamentorescisao' not in item else item['dataagendamentorescisao'],
                #'funcoesGratificadas	': None if 'funcoesGratificadas	' not in item else item['funcoesGratificadas	'],
                'dataTerminoContratoTemporario': None if 'dataterminocontratotemporario' not in item else item['dataterminocontratotemporario'],
                'motivoContratoTemporario': None if 'motivocontratotemporario' not in item else item['motivocontratotemporario'],
                'tipoInclusaoContratoTemporario': None if 'tipoinclusaocontratotemporario' not in item else item['tipoinclusaocontratotemporario'],
                'dataProrrogacaoContratoTemporario': None if 'dataprorrogacaocontratotemporario' not in item else item['dataprorrogacaocontratotemporario'],
                'numeroCartaoPonto': None if 'numerocartaoponto' not in item else item['numerocartaoponto'],
                'indicativoProvimento': None if 'indicativoprovimento' not in item else item['indicativoprovimento'],
                'matriculaEmpresaOrigem': None if 'matriculaempresaorigem' not in item else item['matriculaempresaorigem'],
                'dataAdmissaoOrigem': None if 'dataadmissaoorigem' not in item else item['dataadmissaoorigem'],
                'ocorrenciaSefip': None if 'ocorrenciasefip' not in item else item['ocorrenciasefip'],
                'controleJornada': None if 'controlejornada' not in item else item['controlejornada'],
                'processaAverbacao': None if 'processaaverbacao' not in item else item['processaaverbacao'],
                'dataFinal': None if 'datafinal' not in item else item['datafinal'],
                'dataProrrogacao': None if 'dataprorrogacao' not in item else item['dataprorrogacao'],
                'formacaoPeriodo': None if 'formacaoperiodo' not in item else item['formacaoperiodo'],
                'formacaoFase': None if 'formacaofase' not in item else item['formacaofase'],
                'estagioObrigatorio': None if 'estagioobrigatorio' not in item else item['estagioobrigatorio'],
                'objetivo': None if 'objetivo' not in item else item['objetivo'],
                'numeroContrato': None if 'numerocontrato' not in item else item['numerocontrato'],
                'possuiSeguroVida': None if 'possuisegurovida' not in item else item['possuisegurovida'],
                'numeroApoliceSeguroVida': None if 'numeroapolicesegurovida' not in item else item['numeroapolicesegurovida'],
                #'responsaveis': None if 'responsaveis' not in item else item['responsaveis'],
                'dataCessacaoAposentadoria': None if 'datacessacaoaposentadoria' not in item else item['datacessacaoaposentadoria'],
                'motivoInicioBeneficio': None if 'motivoiniciobeneficio' not in item else item['motivoiniciobeneficio'],
                'duracaoBeneficio': None if 'duracaobeneficio' not in item else item['duracaobeneficio'],
                'dataCessacaoBeneficio': None if 'datacessacaobeneficio' not in item else item['datacessacaobeneficio'],
                'dataInicioContrato': None if 'datainiciocontrato' not in item else item['datainiciocontrato'],
                'situacao': None if 'situacao' not in item else item['situacao'],
                'inicioVigencia': None if 'iniciovigencia' not in item else item['iniciovigencia'],
                'tipo': None if 'tipo' not in item else item['tipo'],
                #'codigoMatricula': None if 'codigoMatricula' not in item else item['codigoMatricula'],
                'eSocial': None if 'eeocial' not in item else item['eeocial'],
                'rendimentoMensal': None if 'rendimentomensal' not in item else item['rendimentomensal'],
                'validationStatus': None if 'validationstatus' not in item else item['validationstatus'],
                'lotacoesFisicas': None if 'lotacoesfisicas' not in item else item['lotacoesfisicas'],
                'historicos': None if 'historicos' not in item else item['historicos']
            }
        }
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
