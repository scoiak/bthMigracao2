select DISTINCT 300 as sistema,
        'matricula' as tipo_registro, 
        1 as chave_dsk1,
        fun.fcncodigo as chave_dsk2,
        'FUNCIONARIO' as tipo,
		/*TO_CHAR(current_date, 'YYYY-MM-DD HH:MM:SSSS:MMMM')*/ 'AJUSTAR' as dataAlteracao, --ajustar
		'AJUSTAR' as origemHistorico,
		(contrato.fundataadmissao) as dataInicioContrato,		
		CASE WHEN contrato.funsituacao = 1 THEN 'TRABALHANDO' 
		 			 WHEN contrato.funsituacao = 2 THEN 'AFASTADO'
		 			 ELSE 'DEMITIDO' END
		as situacao,
		 'AJUSTAR (mesma dataAlteracao)' as inicioVigencia,
		 'AJUSTAR' as dtExercicio,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','organograma', '708',regexp_replace(custo.cncclassif, '[\.]', '', 'gi') ))) as organograma,
		 replace(replace(replace(u.unicpfcnpj,'/',''),'-',''),'.','') as cpfPessoa,
		 null as eSocial,
		 null as grupoFuncional,    
		 (hs.fhssalario) as rendimentoMensal,
		 case when contrato.funformapagamento = 2 then 'CREDITO_EM_CONTA'
			  when contrato.funformapagamento = 3 then 'DINHEIRO'
			  when contrato.funformapagamento = 4 then 'CHEQUE'
			  else 'DINHEIRO' end  formaPagamento,
        null as descricao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'banco', banco.bcocodigo))) as banco, contabanco.bcaagencia,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'agencia-bancaria', banco.bcocodigo, substring(lpad(cast(contabanco.bcaagencia as varchar),4,'0'),1,3), substring(lpad(cast(contabanco.bcaagencia as varchar),4,'0'),4,1))))as agenciaConta,
		contabanco.ifcnumeroconta||contabanco.ifcdigitoconta as contaBancaria,
		public.bth_get_id_gerado('300', 'contas-bancarias',
								 cast(public.bth_get_id_gerado('300', 'pessoa-fisica', cast (contrato.unicodigo as text)) as text),
								 cast(public.bth_get_id_gerado('300', 'agencia-bancaria',cast (contabanco.bcaagencia as text))as text),
								 contabanco.ifcnumeroconta||contabanco.ifcdigitoconta,
								 case when contabanco.ifcativo = 1 THEN 'ABERTA'
								 else 'ENCERRADA' end ) as contaBancariaPagamento,
		contabanco.ifcnumeroconta as numeroConta,
		contabanco.ifcdigitoconta as digitoConta,
		case when contabanco.ifcativo = 1 THEN 'ABERTA'
			else 'ENCERRADA' end as situacaoConta,
	    'false' as historico ,
		contrato.fundataposse as dataBase,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'vinculo-empregaticio', cast(contrato.regcodigo as text)))) as vinculoEmpregaticio,
		--MAX(fun.odomesano),
		case contrato.regcodigo when 24 then 
		   'true'
		 else
		   'false'
		 end  as contratoTemporario,
		'NORMAL' AS indicativoAdmissao,
		'URBANO' as naturezaAtividade,
		'ADMISSAO' as tipoAdmissao,
		 case  contrato.funtipoemprego when 1 then
		   'true'
		 else 
		   'false'
		 end as primeiroEmprego,
		 case contrato.regcodigo when 1 then 
		   'true'
		 else
		   'false'
		 end as optanteFgts,
		 contrato.fundataopcaofgts as dataOpcao,
		 contrato.bcocodigofgts as contaFgts,
		 'null' AS sindicato,
		 'null' AS tipoProvimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (CAST(textojur.txjnumero as text) || '/' || CAST(textojur.txjano as text)), cat.tctdescricao)) ) as leiContrato,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'concurso', 
																									contrato.txjcodigo, 
																									case textojur.asscodigo 
																									   when '60' then 'PROCESSO_SELETIVO'
																									   when 58 then 'CONCURSO_PUBLICO'
																									end))) as concurso_aux,
		contrato.txjcodigo,
		contrato.fundatanomeacao as dataNomeacao,
		contrato.fundataposse as dataPosse,
		case previdencia.tpvcodigo 
		 when 1 then 'true' 
		 when 6 then 'true' 
		else 'false' 
		end as previdenciaFederal,
		case previdencia.tpvcodigo 
		 when 3 then 'true' 
		else 'false' 
		end as previdenciaEstadual,		
		case previdencia.tpvcodigo 
		 when 4 then 'true' 
		else 'false' 
		end as fundoAssistencia,			
		case previdencia.tpvcodigo 
		 when 5 then 'true' 
		else 'false' 
		end as fundoPrevidencia,		
		case previdencia.tpvcodigo 
		 when 2 then 'true' 
		else 'false' 
		end as fundoFincanceiro,
		null as atoAlteracaoCargo,
		null as motivoAlteracaoCargo,
		null as areaAtuacao,
		null as motivoAlteracaoAreaAtuacao,
		case contrato.funocupavaga 
		 when 1 then 'true'
		else 'false' end as ocupaVaga,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (CAST(contrato.txjcodigo as text) || '/' || CAST(textojur.txjano as text)), cat.tctdescricao)) ) as atoContrato,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (CAST(hs.txjcodigo as text) || '/' || CAST(textojur.txjano as text)), cat.tctdescricao)) ) as atoAlteracaoSalario,
		null as atoAlteracaoSalarioComissionado, -- ver depois
		null as motivoAlteracaoSalario, -- ver depois
		null as motivoAlteracaoSalarioComissionado, -- ver depois
		'CARGO' AS origemSalario,
		hs.nivcodigo AS codNivel,
		null as planoSalarial,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', hs.nivcodigo))) as nivelSalarial,
		null as classeSalarial, -- ver depois
		null as referenciaSalarial, -- ver depois
		null as classeReferencia, -- ver depois
		hist_cargo.fcadatafinal as dataSaidaCargo, 
		null --(select max(hist_cargo2.fcadatafinal) from wfp.tbfuncargo as hist_cargo2  
		 --where contrato.fcncodigo=hist_cargo2.fcncodigo and 
		 --      contrato.odomesano = hist_cargo2.odomesano and
		 --      contrato.funcontrato=hist_cargo2.funcontrato )  		 
        as dataSaidaCargoAnterior, --ver depois
		case cargo.cartipocargo when 1 then 1 else 2 end as classifCargo,
		null as classifCargoAnterior, --ver depois
		contrato.carcodigo as codCargo,
		null as codCargoAnterior, --ver depois
		null as cargoAux, --ver depois
		contrato.fcncodigo as cargo, --ver depois
		case cargo.cartipocargo when 1 then NULL else contrato.carcodigo end as cargoComissionado, 
		null as areaAtuacaoComissionado,
		'false' as ocupaVagaComissionado,
		case cargo.cartipocargo when 1 then NULL else hs.fhssalario end as salarioComissionado,
		'MENSALISTA' as unidadePagamento,
		1  as configuracaoFerias, --ver depois
		cargo.carhorastrabmes as quantidadeHorasMes,
		cargo.carhorastrabsem as quantidadeHorasSemana,
		'false' as jornadaParcial, 
		null as dataAgendamentoRescisao,
		null as dataTerminoContratoTemporario, --ver depois
		null as dataProrrogacaoContratoTemporario, --ver depois
		null as tipoInclusaoContratoTemporario, --ver depois
		null as jornadaTrabalho, --
		contrato.funcartaoponto as numeroCartaoPonto,
		case contrato.funnocivos
                               when 0 then ''
                               when 1 then 'NUNCA_EXPOSTO_AGENTES_NOCIVOS'
                               when 2 then 'EXPOSTO_APOSENTADORIA_15_ANOS'
                               when 5 then 'EXPOSTO_APOSENTADORIA_15_ANOS'
                               when 3 then 'EXPOSTO_APOSENTADORIA_20_ANOS'
                               when 7 then 'EXPOSTO_APOSENTADORIA_20_ANOS'
                               when 4 then 'EXPOSTO_APOSENTADORIA_25_ANOS'
                               when 8 then 'EXPOSTO_APOSENTADORIA_25_ANOS'
                          else
                              ''
                          end as ocorrenciaSefip
from wfp.tbfuncontrato as contrato 
left outer join wfp.tbfuncionario as fun on contrato.fcncodigo = fun.fcncodigo and fun.odomesano=contrato.odomesano
inner join wfp.tbfunhistoricosalarial as hs  on hs.fcncodigo = contrato.fcncodigo and fun.odomesano=hs.odomesano
inner join wun.tbunico as u on contrato.unicodigo = u.unicodigo 
inner join wun.tbunicocontabanco as contabanco  on contabanco.unicodigo = contrato.unicodigo  
inner join wun.tbbanco as banco on contabanco.bcocodigo = banco.bcocodigo 
inner join wfp.tbregime as regime on contrato.regcodigo = regime.regcodigo and contrato.odomesano=regime.odomesano
inner join wun.tbcencus  as  custo on custo.cnccodigo = contrato.cnccodigo  
inner join wfp.tbcargo as cargo on  contrato.carcodigo = cargo.carcodigo and contrato.odomesano = cargo.odomesano 
left outer join wfp.tbconcurso as concurso on contrato.txjcodigo =  concurso.txjcodigo and concurso.odomesano=contrato.odomesano
left outer join wlg.tbtextojuridico as textojur on concurso.txjcodigo = textojur.txjcodigo 
left outer join wlg.tbcategoriatexto cat on (cat.tctcodigo = textojur.tctcodigo)
left outer join wlg.tbmovimentotexto mvto on (mvto.txjcodigo = textojur.txjcodigo and mvto.movtipo = 2)
left outer join wlg.tbpublicacao pub on (pub.txjcodigo = textojur.txjcodigo)
left outer join wfp.tbfunpreviden as previdencia on contrato.fcncodigo =  previdencia.fcncodigo and contrato.odomesano = previdencia.odomesano 
left outer join wfp.tbfuncargo as hist_cargo on contrato.fcncodigo=hist_cargo.fcncodigo and  contrato.odomesano = hist_cargo.odomesano and hist_cargo.fcadatafinal is not null and contrato.funcontrato=hist_cargo.funcontrato	 
where /*contrato.fcncodigo IN(8,17242)
and */contrato.odomesano = 202009 
--limit 10 OFFSET 1
