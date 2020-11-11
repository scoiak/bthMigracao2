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
    if False:
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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['codigoMatricula']['numero'],
                                              item['codigoMatricula']['contrato'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Matricula',
            'id_gerado': item['id'],
            'i_chave_dsk1': item['codigoMatricula']['numero'],
            'i_chave_dsk2': item['codigoMatricula']['contrato'],
        })
    # model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
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
            'conteudo': {}
        }
        # 'funcoesGratificadas	': None if 'funcoesGratificadas	' not in item else item['funcoesGratificadas']
        # 'responsaveis': None if 'responsaveis' not in item else item['responsaveis']
        # 'validationStatus': None if 'validationstatus' not in item else item['validationstatus']
        # 'lotacoesFisicas': None if 'lotacoesfisicas' not in item else item['lotacoesfisicas']
        # 'contratoTemporario': None if 'contratotemporario' not in item else item['contratotemporario']
        if 'previdencias' in item and item['previdencias'] is not None:
            dict_dados['conteudo'].update({
                'previdencias': []
            })
            campo = item['previdencias'].split('%|%')
            dict_dados['conteudo']['previdencias'].append({
                # 'matricula': campo[0],
                'tipo': campo[0],
                'plano': {
                    'id': int(campo[1])
                }
            })
        if 'vinculoempregaticio' in item and item['vinculoempregaticio'] is not None:
            dict_dados['conteudo'].update({
                'vinculoEmpregaticio': {
                    'id': item['vinculoempregaticio']
                },
            })
        if 'cargo' in item and item['cargo'] is not None:
            dict_dados['conteudo'].update({
                'cargo': {
                    'id': int(item['cargo'])
                }
            })
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
            dict_dados['conteudo'].update(
                {'dataProrrogacaoContratoTemporario': item['dataprorrogacaocontratotemporario']})
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
                    'id': int(item['nivelsalarial'])
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
        if item['historicos'] is not None:
            listahistorico = []
            lista = item['historicos'].split('%&&%')
            if len(lista) > 0:
                for listacampo in lista:
                    campo = listacampo.split('%&%')
                    for idx, val in enumerate(campo):
                        if campo[idx] == '':
                            campo[idx] = None
                    dict_item_historico = {}
                    # 'funcoesGratificadas	': None if 'funcoesGratificadas	' not in item else item['funcoesGratificadas']
                    # 'responsaveis': None if 'responsaveis' not in item else item['responsaveis']
                    # 'validationStatus': None if 'validationstatus' not in item else item['validationstatus']
                    # 'lotacoesFisicas': None if 'lotacoesfisicas' not in item else item['lotacoesfisicas']
                    # 'contratoTemporario': None if 'contratotemporario' not in item else item['contratotemporario']
                    if campo[18] is not None:
                        dict_item_historico.update({
                            'previdencias': []
                        })
                        campoprevidencia = campo[18].split('%|%')
                        dict_item_historico['previdencias'].append({
                            # 'matricula': campoprevidencia[0],
                            'tipo': campoprevidencia[0],
                            'plano': {
                                'id': int(campoprevidencia[1])
                            }
                        })
                    if campo[1] is not None:
                        dict_item_historico.update({
                            'vinculoEmpregaticio': {
                                'id': campo[1]
                            },
                        })
                    if campo[19] is not None:
                        dict_item_historico.update({
                            'cargo': {
                                'id': int(campo[19])
                            }
                        })
                    if campo[0] is not None:
                        dict_item_historico.update({'dataBase': campo[0]})
                    if campo[71] is not None:
                        dict_item_historico.update({'numeroApoliceSeguroVida': campo[71]})
                    if campo[70] is not None:
                        dict_item_historico.update({'possuiSeguroVida': campo[70]})
                    if campo[27] is not None:
                        dict_item_historico.update({'origemSalario': campo[27]})
                    if campo[26] is not None:
                        dict_item_historico.update({'salarioAlterado': campo[26]})
                    if campo[3] is not None:
                        dict_item_historico.update({'indicativoAdmissao': campo[3]})
                    if campo[94] is not None:
                        dict_item_historico.update({'rendimentoMensal': campo[94]})
                    if campo[4] is not None:
                        dict_item_historico.update({'naturezaAtividade': campo[4]})
                    if campo[5] is not None:
                        dict_item_historico.update({'tipoAdmissao': campo[5]})
                    if campo[6] is not None:
                        dict_item_historico.update({'primeiroEmprego': campo[6]})
                    if campo[11] is not None:
                        dict_item_historico.update({'tipoProvimento': campo[11]})
                    if campo[14] is not None:
                        dict_item_historico.update({'dataNomeacao': campo[14]})
                    if campo[15] is not None:
                        dict_item_historico.update({'dataPosse': campo[15]})
                    if campo[16] is not None:
                        dict_item_historico.update({'tempoAposentadoria': campo[16]})
                    if campo[17] is not None:
                        dict_item_historico.update({'previdenciaFederal': campo[17]})
                    if campo[20] is not None:
                        dict_item_historico.update({'cargoAlterado': campo[20]})
                    if campo[23] is not None:
                        dict_item_historico.update({'areaAtuacaoAlterada': campo[23]})
                    if campo[25] is not None:
                        dict_item_historico.update({'ocupaVaga': campo[25]})
                    if campo[32] is not None:
                        dict_item_historico.update({'ocupaVagaComissionado': campo[32]})
                    if campo[33] is not None:
                        dict_item_historico.update({'salarioComissionado': campo[33]})
                    if campo[36] is not None:
                        dict_item_historico.update({'unidadePagamento': campo[36]})
                    if campo[37] is not None:
                        dict_item_historico.update({'formaPagamento': campo[37]})
                    if campo[40] is not None:
                        dict_item_historico.update({'quantidadeHorasMes': campo[40]})
                    if campo[41] is not None:
                        dict_item_historico.update({'quantidadeHorasSemana': campo[41]})
                    if campo[42] is not None:
                        dict_item_historico.update({'jornadaParcial': campo[42]})
                    if campo[43] is not None:
                        dict_item_historico.update({'dataAgendamentoRescisao': campo[43]})
                    if campo[45] is not None:
                        dict_item_historico.update(
                            {'dataTerminoContratoTemporario': campo[45]})
                    if campo[46] is not None:
                        dict_item_historico.update({'motivoContratoTemporario': campo[46]})
                    if campo[47] is not None:
                        dict_item_historico.update(
                            {'tipoInclusaoContratoTemporario': campo[47]})
                    if campo[48] is not None:
                        dict_item_historico.update(
                            {'dataProrrogacaoContratoTemporario': campo[48]})
                    if campo[49] is not None:
                        dict_item_historico.update({'numeroCartaoPonto': campo[49]})
                    if campo[51] is not None:
                        dict_item_historico.update({'indicativoProvimento': campo[51]})
                    if campo[53] is not None:
                        dict_item_historico.update({'matriculaEmpresaOrigem': campo[53]})
                    if campo[54] is not None:
                        dict_item_historico.update({'dataAdmissaoOrigem': campo[54]})
                    if campo[55] is not None:
                        dict_item_historico.update({'ocorrenciaSefip': campo[55]})
                    if campo[56] is not None:
                        dict_item_historico.update({'controleJornada': campo[56]})
                    if campo[59] is not None:
                        dict_item_historico.update({'processaAverbacao': campo[59]})
                    if campo[60] is not None:
                        dict_item_historico.update({'dataFinal': campo[60]})
                    if campo[61] is not None:
                        dict_item_historico.update({'dataProrrogacao': campo[61]})
                    if campo[65] is not None:
                        dict_item_historico.update({'formacaoPeriodo': campo[65]})
                    if campo[66] is not None:
                        dict_item_historico.update({'formacaoFase': campo[66]})
                    if campo[67] is not None:
                        dict_item_historico.update({'estagioObrigatorio': campo[67]})
                    if campo[68] is not None:
                        dict_item_historico.update({'objetivo': campo[68]})
                    if campo[69] is not None:
                        dict_item_historico.update({'numeroContrato': campo[69]})
                    if campo[74] is not None:
                        dict_item_historico.update({'dataCessacaoAposentadoria': campo[74]})
                    if campo[79] is not None:
                        dict_item_historico.update({'motivoInicioBeneficio': campo[79]})
                    if campo[80] is not None:
                        dict_item_historico.update({'duracaoBeneficio': campo[80]})
                    if campo[81] is not None:
                        dict_item_historico.update({'dataCessacaoBeneficio': campo[81]})
                    if campo[85] is not None:
                        dict_item_historico.update({'dataInicioContrato': campo[85]})
                    if campo[86] is not None:
                        dict_item_historico.update({'situacao': campo[86]})
                    if campo[87] is not None:
                        dict_item_historico.update({'inicioVigencia': campo[87]})
                    if campo[88] is not None:
                        dict_item_historico.update({'tipo': campo[88]})
                    if campo[91] is not None:
                        dict_item_historico.update({'eSocial': campo[91]})
                    if campo[90] is not None:
                        campomatricula = campo[90].split('%|%')
                        dict_item_historico.update({
                            'codigoMatricula': {
                                'contrato': int(campomatricula[0]),
                                'digitoVerificador': int(campomatricula[1]),
                                'numero': int(campomatricula[2])
                            }
                        })
                    if False:
                        if 'sindicato' in item and item['sindicato'] is not None:
                            dict_item_historico.update({
                                'sindicato': {
                                    'id': item['sindicato']
                                }
                            })
                    if campo[12] is not None:
                        dict_item_historico.update({
                            'leiContrato': {
                                'id': campo[12]
                            }
                        })
                    if campo[13] is not None:
                        dict_item_historico.update({
                            'atoContrato': {
                                'id': campo[13]
                            }
                        })
                    if campo[21] is not None:
                        dict_item_historico.update({
                            'atoAlteracaoCargo': {
                                'id': campo[21]
                            }
                        })
                    if campo[22] is not None:
                        dict_item_historico.update({
                            'areaAtuacao': {
                                'id': campo[22]
                            }
                        })
                    if campo[24] is not None:
                        dict_item_historico.update({
                            'motivoAlteracaoAreaAtuacao': {
                                'id': campo[24]
                            }
                        })
                    if campo[28] is not None:
                        dict_item_historico.update({
                            'nivelSalarial': {
                                'id': int(campo[28])
                            }
                        })
                    if campo[29] is not None:
                        dict_item_historico.update({
                            'classeReferencia': {
                                'id': campo[29]
                            }
                        })
                    if campo[30] is not None:
                        dict_item_historico.update({
                            'cargoComissionado': {
                                'id': campo[30]
                            }
                        })
                    if campo[31] is not None:
                        dict_item_historico.update({
                            'areaAtuacaoComissionado': {
                                'id': campo[31]
                            }
                        })
                    if campo[34] is not None:
                        dict_item_historico.update({
                            'nivelSalarialComissionado': {
                                'id': campo[34]
                            }
                        })
                    if campo[35] is not None:
                        dict_item_historico.update({
                            'classeReferenciaComissionado': {
                                'id': campo[35]
                            }
                        })
                    if False:
                        if 'contabancariapagamento' in item and item['contabancariapagamento'] is not None:
                            dict_item_historico.update({
                                'contaBancariaPagamento': {
                                    'id': item['contabancariapagamento']
                                }
                            })
                    if campo[39] is not None:
                        dict_item_historico.update({
                            'configuracaoFerias': {
                                'id': campo[39]
                            }
                        })
                    if campo[50] is not None:
                        dict_item_historico.update({
                            'parametroPonto': {
                                'id': campo[50]
                            }
                        })
                    if campo[52] is not None:
                        dict_item_historico.update({
                            'orgaoOrigem': {
                                'id': campo[52]
                            }
                        })
                    if campo[57] is not None:
                        dict_item_historico.update({
                            'configuracaoLicencaPremio': {
                                'id': campo[57]
                            }
                        })
                    if campo[58] is not None:
                        dict_item_historico.update({
                            'configuracaoAdicional': {
                                'id': campo[58]
                            }
                        })
                    if campo[62] is not None:
                        dict_item_historico.update({
                            'instituicaoEnsino': {
                                'id': campo[62]
                            }
                        })
                    if campo[63] is not None:
                        dict_item_historico.update({
                            'agenteIntegracao': {
                                'id': campo[63]
                            }
                        })
                    if campo[64] is not None:
                        dict_item_historico.update({
                            'formacao': {
                                'id': campo[64]
                            }
                        })
                    if campo[72] is not None:
                        dict_item_historico.update({
                            'categoriaTrabalhador': {
                                'id': campo[72]
                            }
                        })
                    if campo[76] is not None:
                        dict_item_historico.update({
                            'motivoAposentadoria': {
                                'id': campo[76]
                            }
                        })
                    if campo[77] is not None:
                        dict_item_historico.update({
                            'funcionarioOrigem': {
                                'id': campo[77]
                            }
                        })
                    if campo[78] is not None:
                        dict_item_historico.update({
                            'tipoMovimentacao': {
                                'id': campo[78]
                            }
                        })
                    if campo[83] is not None:
                        dict_item_historico.update({
                            'matriculaOrigem': {
                                'id': campo[83]
                            }
                        })
                    if campo[84] is not None:
                        dict_item_historico.update({
                            'responsavel': {
                                'id': campo[84]
                            }
                        })
                    if campo[89] is not None:
                        dict_item_historico.update({
                            'pessoa': {
                                'id': campo[89]
                            }
                        })
                    if campo[92] is not None:
                        dict_item_historico.update({
                            'grupoFuncional': {
                                'id': campo[92]
                            }
                        })
                    if campo[93] is not None:
                        dict_item_historico.update({
                            'jornadaTrabalho': {
                                'id': campo[93]
                            }
                        })
                    if campo[95] is not None:
                        dict_item_historico.update({
                            'atoAlteracaoSalario': {
                                'id': campo[95]
                            }
                        })
                    if campo[96] is not None:
                        dict_item_historico.update({
                            'motivoAlteracaoSalario': {
                                'id': campo[98]
                            }
                        })
                    if campo[98] is not None:
                        dict_item_historico.update({
                            'organograma': {
                                'id': campo[98]
                            }
                        })
                    listahistorico.append(dict_item_historico)
            if len(listahistorico) > 0:
                dict_dados['conteudo'].update({
                    'historicos': listahistorico
                })
        contador += 1
        print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Matricula',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['fcncodigo'],
            'i_chave_dsk2': item['funcontrato'],
        })
    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
    if True:
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)
        print('- Envio de dados finalizado.')