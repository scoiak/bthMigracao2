create index IF NOT exists idx_f_fhs on wfp.tbfunhistoricosalarial (fcncodigo, funcontrato, odomesano);
create index IF NOT exists idx_f_fc on wfp.tbfuncontrato (fcncodigo, odomesano);
create index IF NOT exists idx_fc_f on wfp.tbfuncionario (fcncodigo, odomesano);
create index IF NOT EXISTs idx_fp_fc on wfp.tbfunpreviden (fcncodigo, funcontrato);
create index IF NOT exists idx_rc_fc on wfp.tbrescisaocalculada (fcncodigo, funcontrato);
create index IF NOT exists idx_pc_fc on wfp.tbprorrogacontr (fcncodigo, funcontrato);
 
-- DROP TABLE IF EXISTS matricula CASCADE;

-- SELECT string_agg('coalesce(suc.' || column_name || '::varchar,'''')',' || ''%&%'' ||')  FROM information_schema.columns WHERE  table_name   = 'matricula';

/*
select * 
INTO TABLE matricula
from (select 
	(case when fc.funtipocontrato not in (2) then fundataadmissao::varchar else null end) as database, --0	
	(case when fc.funtipocontrato not in (2,3,4) then (coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio',regcodigo::varchar))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio','1'))))) else null end)::varchar as vinculoEmpregaticio,
	(case fc.regcodigo when 24 then 'true' when 20 then 'true' else 'false' end) as contratoTemporario, --2
	(case when fc.funtipocontrato not in (2) then 'NORMAL' else null end) as indicativoAdmissao,
	'URBANO' as naturezaAtividade,
	(case funtipocontrato when 10 then 'TRANSFERENCIA' else 'ADMISSAO' end) as tipoAdmissao,	--5
	 (case funtipoemprego when 1 then 'true' else  'false' end) as primeiroEmprego, --6
	 (case regcodigo when 1 then  'true' else	'false'	 end) as optanteFgts,
	fundataopcaofgts::varchar as dataOpcao,
	ifcsequenciafgts as contaFgts,
	unicodigocsi as sindicato,--10
	null as tipoProvimento,
	null as leiContrato,
	--txjcodigo as atoContrato,
	null as atoContrato,
	fundatanomeacao::varchar as dataNomeacao,
	fundataposse::varchar as dataPosse,--15
	null as tempoAposentadoria,
	(case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano limit 1) when 1 then true when 6 then true else false end) as previdenciaFederal,
	--true as previdenciaFederal,
	(case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano limit 1) when 1 then null when 6 then null else ((case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato limit 1) when 3 then 'ESTADUAL' when 4 then 'FUNDO_ASSISTENCIA' when 5 then 'FUNDO_PREVIDENCIA' when 2 then 'FUNDO_FINANCEIRO' else null end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'plano-previdencia', (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato limit 1))))) end) as previdencias,
	(case when fc.funtipocontrato not in (4) then coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', carcodigo))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', '1')))) else null end)::varchar as cargo,--19
	null as cargoAlterado,--20
	--txjcodigo as atoAlteracaoCargo,
	null as atoAlteracaoCargo,
	null as areaAtuacao,
	null as areaAtuacaoAlterada,
	null as motivoAlteracaoAreaAtuacao,
	(case funocupavaga when 1 then true else false end) as ocupaVaga,--25
	null as salarioAlterado,
	null as origemSalario,	
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', nivcodigo)))::varchar as nivelSalarial,
	null as classeReferencia,
	null as cargoComissionado,--30
	null as areaAtuacaoComissionado,
	false as ocupaVagaComissionado,
	null as salarioComissionado,
	null as nivelSalarialComissionado,
	null as classeReferenciaComissionado,--35
	'MENSALISTA' as unidadePagamento,
	--(case funformapagamento when 2 then 'CREDITO_EM_CONTA'  when 3 then 'DINHEIRO' when 4 then 'CHEQUE' else 'DINHEIRO' end) as formaPagamento,
	'DINHEIRO' as formaPagamento,
	ifcsequenciapaga as contaBancariaPagamento,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias', 1))) as configuracaoFerias,
	cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer) as quantidadeHorasMes,--40
	(cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer)/5) as quantidadeHorasSemana,
	false as jornadaParcial,
	null as dataAgendamentoRescisao,
	null as funcoesGratificadas, 
	(case fc.funsituacao when 1 then null else (case fc.regcodigo when 24 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) when 20 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) else null end) end)::varchar as dataTerminoContratoTemporario,
	null as motivoContratoTemporario,
	null as tipoInclusaoContratoTemporario,
	null as dataProrrogacaoContratoTemporario,
	funcartaoponto as numeroCartaoPonto,
	null as parametroPonto,--50
	null as indicativoProvimento,
	null as orgaoOrigem,
	null as matriculaEmpresaOrigem,
	null as dataAdmissaoOrigem,
	(case fc.funnocivos   when 0 then null   when 1 then 'NUNCA_EXPOSTO_AGENTES_NOCIVOS'  when 2 then 'EXPOSTO_APOSENTADORIA_15_ANOS'  when 5 then 'EXPOSTO_APOSENTADORIA_15_ANOS'     when 3 then 'EXPOSTO_APOSENTADORIA_20_ANOS'  when 7 then 'EXPOSTO_APOSENTADORIA_20_ANOS'   when 4 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    when 8 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    else     null    end) as ocorrenciaSefip,--55
	null as controleJornada,--56
	null as configuracaoLicencaPremio,
	null as configuracaoAdicional,
	null as processaAverbacao,
	null as dataFinal,--60
	null as dataProrrogacao,
	null as instituicaoEnsino,
	null as agenteIntegracao,
	null as formacao,
	null as formacaoPeriodo,--65
	null as formacaoFase,
	null as estagioObrigatorio,
	null as objetivo,
	fc.funcontrato as numeroContrato,
	null as possuiSeguroVida,--70
	null as numeroApoliceSeguroVida,
	null as categoriaTrabalhador,
	null as responsaveis,
	null as dataCessacaoAposentadoria,
	null as entidadeOrigem,--75
	null as motivoAposentadoria,
	null as funcionarioOrigem,
	null as tipoMovimentacao,
	null as motivoInicioBeneficio,
	null as duracaoBeneficio,--80
	null as dataCessacaoBeneficio,
	null as motivoCessacaoBeneficio,
	null as matriculaOrigem,
	null as responsavel,
	fundataadmissao::varchar as dataInicioContrato,--85
	(CASE fc.funsituacao WHEN  1 THEN 'TRABALHANDO'  WHEN 2 THEN 'AFASTADO' ELSE 'DEMITIDO' end) as situacao,
	--'DEMITIDO' as situacao,
	--'2020-11-01 00:00:00' as inicioVigencia,
	(SELECT to_date(fc.odomesano||'01','YYYYMMDD')::varchar || ' 00:00:00')  as inicioVigencia,
	--(CASE fc.funtipocontrato WHEN  1 THEN 'FUNCIONARIO'  	WHEN 2 THEN 'ESTAGIARIO' 	WHEN 3 THEN 'PENSIONISTA'	WHEN 4 THEN 'APOSENTADO'	WHEN 5 THEN 'REINTEGRACAO'	WHEN 6 THEN 'TRANSFERENCIA'	WHEN 7 THEN 'MENOR_APRENDIZ'	WHEN 8 THEN 'CEDIDO'	WHEN 9 THEN 'CEDIDO'	WHEN 10 THEN 'RECEBIDO'	WHEN 11 THEN 'RECEBIDO'	WHEN 12 THEN 'PREVIDENCIA'	ELSE 'FUNCIONARIO' end) as tipo,
	(CASE fc.funtipocontrato WHEN  1 THEN 'FUNCIONARIO'  	WHEN 2 THEN 'ESTAGIARIO' 	WHEN 3 THEN 'PENSIONISTA'	WHEN 4 THEN 'APOSENTADO'	WHEN 5 THEN 'FUNCIONARIO'	WHEN 6 THEN 'FUNCIONARIO'	WHEN 7 THEN 'FUNCIONARIO'	WHEN 8 THEN 'FUNCIONARIO'	WHEN 9 THEN 'FUNCIONARIO'	WHEN 10 THEN 'FUNCIONARIO'	WHEN 11 THEN 'FUNCIONARIO'	WHEN 12 THEN 'FUNCIONARIO'	ELSE 'FUNCIONARIO' end) as tipo, 
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(u.unicpfcnpj,'[/.-]','','g') from  wun.tbunico u where u.unicodigo  = fc.unicodigo limit 1))))  as pessoa,
	(fc.funcontrato || '%|%' || '0' || '%|%' || fc.fcncodigo ) as codigoMatricula,--90
	null as eSocial,
	null as grupoFuncional,
	null as jornadaTrabalho,
	-- coalesce(funsalariobase,(select fhssalario from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc limit 1)) as rendimentoMensal,
	coalesce((case when funsalariobase < 1 then null else funsalariobase end),(select (case when nivsalariobase < 1 then null else nivsalariobase end) from wfp.tbnivel n where n.nivcodigo = fc.nivcodigo and n.odomesano = fc.odomesano),1.0) as rendimentoMensal,--94
	(select txjcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc  limit 1) as atoAlteracaoSalario,--95
	--(select mtrcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc limit 1)  as motivoAlteracaoSalario,
	null as motivoAlteracaoSalario, 
	null as validationStatus,		
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','configuracao-organograma', 1)))::varchar,(select left((c.organo::varchar || regexp_replace(c.cncclassif, '[\.]', '', 'gi') || '000000000000000'),15) from wun.tbcencus as c where c.cnccodigo = fc.cnccodigo limit 1)::varchar))) as organograma,
	null as lotacoesFisicas,--99
	null as historicos,--100
	row_number() over() as id,
	fc.fcncodigo,
	fc.funcontrato,
	fc.odomesano as competencia
from wfp.tbfuncontrato as fc  join wfp.tbfuncionario as f on f.fcncodigo = fc.fcncodigo and f.odomesano = fc.odomesano
) as s
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', codigoMatricula, numeroContrato))) is null
and pessoa is not null;
*/

create index if not exists idx_m_fc on matricula (fcncodigo, funcontrato);

select * 
from (select 	
	(case when fc.funtipocontrato not in (2) then fundataadmissao::varchar else null end) as database, --0		
	(case when fc.funtipocontrato not in (2,3,4) then (coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio',regcodigo::varchar))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio','1'))))) else null end)::varchar as vinculoEmpregaticio,
	(case fc.regcodigo when 24 then 'true' when 20 then 'true' else 'false' end) as contratoTemporario, --2
	(case when fc.funtipocontrato not in (2) then 'NORMAL' else null end) as indicativoAdmissao,
	'URBANO' as naturezaAtividade,
	(case funtipocontrato when 10 then 'TRANSFERENCIA' else 'ADMISSAO' end) as tipoAdmissao,	--5
	 (case funtipoemprego when 1 then 'true' else  'false' end) as primeiroEmprego, --6
	 (case regcodigo when 1 then  'true' else	'false'	 end) as optanteFgts,
	fundataopcaofgts::varchar as dataOpcao,
	ifcsequenciafgts as contaFgts,
	unicodigocsi as sindicato,--10
	null as tipoProvimento,
	null as leiContrato,
	--txjcodigo as atoContrato,
	null as atoContrato,
	fundatanomeacao::varchar as dataNomeacao,
	fundataposse::varchar as dataPosse,--15
	null as tempoAposentadoria,
	(case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano limit 1) when 1 then true when 6 then true else false end) as previdenciaFederal,
	--true as previdenciaFederal,
	--(case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato limit 1) when 3 then 'ESTADUAL' when 4 then 'FUNDO_ASSISTENCIA' when 5 then 'FUNDO_PREVIDENCIA' when 2 then 'FUNDO_FINANCEIRO' else null end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'plano-previdencia', (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato limit 1)))) as previdencias,
	(case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano limit 1) when 1 then null when 6 then null else ((case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato limit 1) when 3 then 'ESTADUAL' when 4 then 'FUNDO_ASSISTENCIA' when 5 then 'FUNDO_PREVIDENCIA' when 2 then 'FUNDO_FINANCEIRO' else null end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'plano-previdencia', (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato limit 1))))) end) as previdencias,
	(case when fc.funtipocontrato not in (4) then coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', carcodigo))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', '1')))) else null end)::varchar as cargo,--19
	null as cargoAlterado,--20
	--txjcodigo as atoAlteracaoCargo,
	null as atoAlteracaoCargo,
	null as areaAtuacao,
	null as areaAtuacaoAlterada,
	null as motivoAlteracaoAreaAtuacao,
	(case funocupavaga when 1 then true else false end) as ocupaVaga,--25
	null as salarioAlterado,
	null as origemSalario,	
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', nivcodigo)))::varchar as nivelSalarial,
	null as classeReferencia,
	null as cargoComissionado,--30
	null as areaAtuacaoComissionado,
	false as ocupaVagaComissionado,
	null as salarioComissionado,
	null as nivelSalarialComissionado,
	null as classeReferenciaComissionado,--35
	'MENSALISTA' as unidadePagamento,
	--(case funformapagamento when 2 then 'CREDITO_EM_CONTA'  when 3 then 'DINHEIRO' when 4 then 'CHEQUE' else 'DINHEIRO' end) as formaPagamento,
	'DINHEIRO' as formaPagamento,
	ifcsequenciapaga as contaBancariaPagamento,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias', 1))) as configuracaoFerias,
	cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer) as quantidadeHorasMes,--40
	(cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer)/5) as quantidadeHorasSemana,
	false as jornadaParcial,
	null as dataAgendamentoRescisao,
	null as funcoesGratificadas, 
	(case fc.funsituacao when 1 then null else (case fc.regcodigo when 24 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) when 20 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) else null end) end)::varchar as dataTerminoContratoTemporario,
	null as motivoContratoTemporario,
	null as tipoInclusaoContratoTemporario,
	null as dataProrrogacaoContratoTemporario,
	funcartaoponto as numeroCartaoPonto,
	null as parametroPonto,--50
	null as indicativoProvimento,
	null as orgaoOrigem,
	null as matriculaEmpresaOrigem,
	null as dataAdmissaoOrigem,
	(case fc.funnocivos   when 0 then null   when 1 then 'NUNCA_EXPOSTO_AGENTES_NOCIVOS'  when 2 then 'EXPOSTO_APOSENTADORIA_15_ANOS'  when 5 then 'EXPOSTO_APOSENTADORIA_15_ANOS'     when 3 then 'EXPOSTO_APOSENTADORIA_20_ANOS'  when 7 then 'EXPOSTO_APOSENTADORIA_20_ANOS'   when 4 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    when 8 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    else     null    end) as ocorrenciaSefip,--55
	null as controleJornada,--56
	null as configuracaoLicencaPremio,
	null as configuracaoAdicional,
	null as processaAverbacao,
	null as dataFinal,--60
	null as dataProrrogacao,
	null as instituicaoEnsino,
	null as agenteIntegracao,
	null as formacao,
	null as formacaoPeriodo,--65
	null as formacaoFase,
	null as estagioObrigatorio,
	null as objetivo,
	fc.funcontrato as numeroContrato,
	null as possuiSeguroVida,--70
	null as numeroApoliceSeguroVida,
	null as categoriaTrabalhador,
	null as responsaveis,
	null as dataCessacaoAposentadoria,
	null as entidadeOrigem,--75
	null as motivoAposentadoria,
	null as funcionarioOrigem,
	null as tipoMovimentacao,
	null as motivoInicioBeneficio,
	null as duracaoBeneficio,--80
	null as dataCessacaoBeneficio,
	null as motivoCessacaoBeneficio,
	null as matriculaOrigem,
	null as responsavel,
	fundataadmissao::varchar as dataInicioContrato,--85
	(CASE fc.funsituacao WHEN  1 THEN 'TRABALHANDO'  WHEN 2 THEN 'AFASTADO' ELSE 'DEMITIDO' end) as situacao,
	--'2020-11-01 00:00:00' as inicioVigencia,
	(SELECT to_date(fc.odomesano||'01','YYYYMMDD')::varchar || ' 00:00:00')  as inicioVigencia,
	--(CASE fc.funtipocontrato WHEN  1 THEN 'FUNCIONARIO'  	WHEN 2 THEN 'ESTAGIARIO' 	WHEN 3 THEN 'PENSIONISTA'	WHEN 4 THEN 'APOSENTADO'	WHEN 5 THEN 'REINTEGRACAO'	WHEN 6 THEN 'TRANSFERENCIA'	WHEN 7 THEN 'MENOR_APRENDIZ'	WHEN 8 THEN 'CEDIDO'	WHEN 9 THEN 'CEDIDO'	WHEN 10 THEN 'RECEBIDO'	WHEN 11 THEN 'RECEBIDO'	WHEN 12 THEN 'PREVIDENCIA'	ELSE 'FUNCIONARIO' end) as tipo,
	(CASE fc.funtipocontrato WHEN  1 THEN 'FUNCIONARIO'  	WHEN 2 THEN 'ESTAGIARIO' 	WHEN 3 THEN 'PENSIONISTA'	WHEN 4 THEN 'APOSENTADO'	WHEN 5 THEN 'FUNCIONARIO'	WHEN 6 THEN 'FUNCIONARIO'	WHEN 7 THEN 'FUNCIONARIO'	WHEN 8 THEN 'FUNCIONARIO'	WHEN 9 THEN 'FUNCIONARIO'	WHEN 10 THEN 'FUNCIONARIO'	WHEN 11 THEN 'FUNCIONARIO'	WHEN 12 THEN 'FUNCIONARIO'	ELSE 'FUNCIONARIO' end) as tipo, 
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(u.unicpfcnpj,'[/.-]','','g') from  wun.tbunico u where u.unicodigo  = fc.unicodigo limit 1))))  as pessoa,
	(fc.funcontrato || '%|%' || '0' || '%|%' || fc.fcncodigo ) as codigoMatricula,--90
	null as eSocial,
	null as grupoFuncional,
	null as jornadaTrabalho,
	-- coalesce(funsalariobase,(select fhssalario from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc limit 1)) as rendimentoMensal,
	coalesce((case when funsalariobase < 1 then null else funsalariobase end),(select (case when nivsalariobase < 1 then null else nivsalariobase end) from wfp.tbnivel n where n.nivcodigo = fc.nivcodigo and n.odomesano = fc.odomesano),1.0) as rendimentoMensal,--94
	(select txjcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc  limit 1) as atoAlteracaoSalario,--95
	--(select mtrcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc limit 1)  as motivoAlteracaoSalario,
	null as motivoAlteracaoSalario, 
	null as validationStatus,		
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','configuracao-organograma', 1)))::varchar,(select left((c.organo::varchar || regexp_replace(c.cncclassif, '[\.]', '', 'gi') || '000000000000000'),15) from wun.tbcencus as c where c.cnccodigo = fc.cnccodigo limit 1)::varchar))) as organograma,
	null as lotacoesFisicas,--99
	(select 	
	string_agg( 
	coalesce(suc.database::varchar,'') || '%&%' ||coalesce(suc.vinculoempregaticio::varchar,'') || '%&%' ||coalesce(suc.contratotemporario::varchar,'') || '%&%' ||coalesce(suc.indicativoadmissao::varchar,'') || '%&%' ||coalesce(suc.naturezaatividade::varchar,'') || '%&%' ||coalesce(suc.tipoadmissao::varchar,'') || '%&%' ||coalesce(suc.primeiroemprego::varchar,'') || '%&%' ||coalesce(suc.optantefgts::varchar,'') || '%&%' ||coalesce(suc.dataopcao::varchar,'') || '%&%' ||coalesce(suc.contafgts::varchar,'') || '%&%' ||coalesce(suc.sindicato::varchar,'') || '%&%' ||coalesce(suc.tipoprovimento::varchar,'') || '%&%' ||coalesce(suc.leicontrato::varchar,'') || '%&%' ||coalesce(suc.atocontrato::varchar,'') || '%&%' ||coalesce(suc.datanomeacao::varchar,'') || '%&%' ||coalesce(suc.dataposse::varchar,'') || '%&%' ||coalesce(suc.tempoaposentadoria::varchar,'') || '%&%' ||coalesce(suc.previdenciafederal::varchar,'') || '%&%' ||coalesce(suc.previdencias::varchar,'') || '%&%' ||coalesce(suc.cargo::varchar,'') || '%&%' ||coalesce(suc.cargoalterado::varchar,'') || '%&%' ||coalesce(suc.atoalteracaocargo::varchar,'') || '%&%' ||coalesce(suc.areaatuacao::varchar,'') || '%&%' ||coalesce(suc.areaatuacaoalterada::varchar,'') || '%&%' ||coalesce(suc.motivoalteracaoareaatuacao::varchar,'') || '%&%' ||coalesce(suc.ocupavaga::varchar,'') || '%&%' ||coalesce(suc.salarioalterado::varchar,'') || '%&%' ||coalesce(suc.origemsalario::varchar,'') || '%&%' ||coalesce(suc.nivelsalarial::varchar,'') || '%&%' ||coalesce(suc.classereferencia::varchar,'') || '%&%' ||coalesce(suc.cargocomissionado::varchar,'') || '%&%' ||coalesce(suc.areaatuacaocomissionado::varchar,'') || '%&%' ||coalesce(suc.ocupavagacomissionado::varchar,'') || '%&%' ||coalesce(suc.salariocomissionado::varchar,'') || '%&%' ||coalesce(suc.nivelsalarialcomissionado::varchar,'') || '%&%' ||coalesce(suc.classereferenciacomissionado::varchar,'') || '%&%' ||coalesce(suc.unidadepagamento::varchar,'') || '%&%' ||coalesce(suc.formapagamento::varchar,'') || '%&%' ||coalesce(suc.contabancariapagamento::varchar,'') || '%&%' ||coalesce(suc.configuracaoferias::varchar,'') || '%&%' ||coalesce(suc.quantidadehorasmes::varchar,'') || '%&%' ||coalesce(suc.quantidadehorassemana::varchar,'') || '%&%' ||coalesce(suc.jornadaparcial::varchar,'') || '%&%' ||coalesce(suc.dataagendamentorescisao::varchar,'') || '%&%' ||coalesce(suc.funcoesgratificadas::varchar,'') || '%&%' ||coalesce(suc.dataterminocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.motivocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.tipoinclusaocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.dataprorrogacaocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.numerocartaoponto::varchar,'') || '%&%' ||coalesce(suc.parametroponto::varchar,'') || '%&%' ||coalesce(suc.indicativoprovimento::varchar,'') || '%&%' ||coalesce(suc.orgaoorigem::varchar,'') || '%&%' ||coalesce(suc.matriculaempresaorigem::varchar,'') || '%&%' ||coalesce(suc.dataadmissaoorigem::varchar,'') || '%&%' ||coalesce(suc.ocorrenciasefip::varchar,'') || '%&%' ||coalesce(suc.controlejornada::varchar,'') || '%&%' ||coalesce(suc.configuracaolicencapremio::varchar,'') || '%&%' ||coalesce(suc.configuracaoadicional::varchar,'') || '%&%' ||coalesce(suc.processaaverbacao::varchar,'') || '%&%' ||coalesce(suc.datafinal::varchar,'') || '%&%' ||coalesce(suc.dataprorrogacao::varchar,'') || '%&%' ||coalesce(suc.instituicaoensino::varchar,'') || '%&%' ||coalesce(suc.agenteintegracao::varchar,'') || '%&%' ||coalesce(suc.formacao::varchar,'') || '%&%' ||coalesce(suc.formacaoperiodo::varchar,'') || '%&%' ||coalesce(suc.formacaofase::varchar,'') || '%&%' ||coalesce(suc.estagioobrigatorio::varchar,'') || '%&%' ||coalesce(suc.objetivo::varchar,'') || '%&%' ||coalesce(suc.numerocontrato::varchar,'') || '%&%' ||coalesce(suc.possuisegurovida::varchar,'') || '%&%' ||coalesce(suc.numeroapolicesegurovida::varchar,'') || '%&%' ||coalesce(suc.categoriatrabalhador::varchar,'') || '%&%' ||coalesce(suc.responsaveis::varchar,'') || '%&%' ||coalesce(suc.datacessacaoaposentadoria::varchar,'') || '%&%' ||coalesce(suc.entidadeorigem::varchar,'') || '%&%' ||coalesce(suc.motivoaposentadoria::varchar,'') || '%&%' ||coalesce(suc.funcionarioorigem::varchar,'') || '%&%' ||coalesce(suc.tipomovimentacao::varchar,'') || '%&%' ||coalesce(suc.motivoiniciobeneficio::varchar,'') || '%&%' ||coalesce(suc.duracaobeneficio::varchar,'') || '%&%' ||coalesce(suc.datacessacaobeneficio::varchar,'') || '%&%' ||coalesce(suc.motivocessacaobeneficio::varchar,'') || '%&%' ||coalesce(suc.matriculaorigem::varchar,'') || '%&%' ||coalesce(suc.responsavel::varchar,'') || '%&%' ||coalesce(suc.datainiciocontrato::varchar,'') || '%&%' ||coalesce(suc.situacao::varchar,'') || '%&%' ||coalesce(suc.iniciovigencia::varchar,'') || '%&%' ||coalesce(suc.tipo::varchar,'') || '%&%' ||coalesce(suc.pessoa::varchar,'') || '%&%' ||coalesce(suc.codigomatricula::varchar,'') || '%&%' ||coalesce(suc.esocial::varchar,'') || '%&%' ||coalesce(suc.grupofuncional::varchar,'') || '%&%' ||coalesce(suc.jornadatrabalho::varchar,'') || '%&%' ||coalesce(suc.rendimentomensal::varchar,'') || '%&%' ||coalesce(suc.atoalteracaosalario::varchar,'') || '%&%' ||coalesce(suc.motivoalteracaosalario::varchar,'') || '%&%' ||coalesce(suc.validationstatus::varchar,'') || '%&%' ||coalesce(suc.organograma::varchar,'') || '%&%' ||coalesce(suc.lotacoesfisicas::varchar,'') || '%&%' ||coalesce(suc.historicos::varchar,'') || '%&%' ||coalesce(suc.id::varchar,'') || '%&%' ||coalesce(suc.fcncodigo::varchar,'') || '%&%' ||coalesce(suc.funcontrato::varchar,'') || '%&%' ||coalesce(suc.competencia::varchar,'')
	,'%&&%')
	from ( select * from (select distinct on (m.vinculoempregaticio,m.contratotemporario,m.indicativoadmissao,m.naturezaatividade,m.tipoadmissao,m.primeiroemprego,m.optantefgts,m.dataopcao,m.contafgts,m.sindicato,m.tipoprovimento,m.leicontrato,m.atocontrato,m.datanomeacao,m.dataposse,m.tempoaposentadoria,m.previdenciafederal,m.previdencias,m.cargo,m.cargoalterado,m.atoalteracaocargo,m.areaatuacao,m.areaatuacaoalterada,m.motivoalteracaoareaatuacao,m.ocupavaga,m.salarioalterado,m.origemsalario,m.nivelsalarial,m.classereferencia,m.cargocomissionado,m.areaatuacaocomissionado,m.ocupavagacomissionado,m.salariocomissionado,m.nivelsalarialcomissionado,m.classereferenciacomissionado,m.unidadepagamento,m.formapagamento,m.contabancariapagamento,m.configuracaoferias,m.quantidadehorasmes,m.quantidadehorassemana,m.jornadaparcial,m.dataagendamentorescisao,m.funcoesgratificadas,m.dataterminocontratotemporario,m.motivocontratotemporario,m.tipoinclusaocontratotemporario,m.dataprorrogacaocontratotemporario,m.numerocartaoponto,m.parametroponto,m.indicativoprovimento,m.orgaoorigem,m.matriculaempresaorigem,m.dataadmissaoorigem,m.ocorrenciasefip,m.controlejornada,m.configuracaolicencapremio,m.configuracaoadicional,m.processaaverbacao,m.datafinal,m.dataprorrogacao,m.instituicaoensino,m.agenteintegracao,m.formacao,m.formacaoperiodo,m.formacaofase,m.estagioobrigatorio,m.objetivo,m.numerocontrato,m.possuisegurovida,m.numeroapolicesegurovida,m.categoriatrabalhador,m.responsaveis,m.datacessacaoaposentadoria,m.entidadeorigem,m.motivoaposentadoria,m.funcionarioorigem,m.tipomovimentacao,m.motivoiniciobeneficio,m.duracaobeneficio,m.datacessacaobeneficio,m.motivocessacaobeneficio,m.matriculaorigem,m.responsavel,m.situacao,m.tipo,m.pessoa,m.codigomatricula,m.esocial,m.grupofuncional,m.jornadatrabalho,m.rendimentomensal,m.atoalteracaosalario,m.motivoalteracaosalario,m.validationstatus,m.organograma,m.lotacoesfisicas,m.historicos) * from matricula as m where m.fcncodigo = fc.fcncodigo and m.funcontrato = fc.funcontrato and m.competencia < fc.odomesano) as sssuc  order by sssuc.competencia asc) as suc) as historicos,
	row_number() over() as id,
	fc.fcncodigo,
	fc.funcontrato,	
	fc.odomesano as competencia
from wfp.tbfuncontrato as fc  join wfp.tbfuncionario as f on f.fcncodigo = fc.fcncodigo and f.odomesano = fc.odomesano
where fc.odomesano = 202009
--and fc.fcncodigo = 15605
and fc.funsituacao in (1,2)
order by fc.fcncodigo,fc.funcontrato
--limit 10 offset 0
) as s
where 
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', codigoMatricula, numeroContrato))) is null and
pessoa is not null;
