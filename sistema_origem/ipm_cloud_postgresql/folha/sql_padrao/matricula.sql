-- DROP index IF exists idx_f_fhs,idx_f_fc,idx_fc_f,idx_fp_fc,idx_rc_fc,idx_pc_fc,idx_cmr,idx_m_fc,idx_ucb_fc,idx_u_fc,idx_uf_fc cascade;
 
create index IF NOT exists idx_f_fhs on wfp.tbfunhistoricosalarial (fcncodigo, funcontrato, odomesano);
create index IF NOT exists idx_f_fc on wfp.tbfuncontrato (fcncodigo, odomesano);
create index IF NOT exists idx_fc_f on wfp.tbfuncionario (fcncodigo, odomesano);
create index IF NOT EXISTs idx_fp_fc on wfp.tbfunpreviden (fcncodigo, funcontrato, odomesano);
create index IF NOT exists idx_rc_fc on wfp.tbrescisaocalculada (fcncodigo, funcontrato);
create index IF NOT exists idx_pc_fc on wfp.tbprorrogacontr (fcncodigo, funcontrato);
create index IF NOT exists idx_cmr on public.controle_migracao_registro (hash_chave_dsk);
create index IF NOT exists idx_ucb_fc on wun.tbunicocontabanco (unicodigo, ifcsequencia);
create index IF NOT exists idx_u_fc on wun.tbunico (unicodigo);
create index IF NOT exists idx_uf_fc on wun.tbunicofisica (unicodigo);

-- DROP TABLE IF EXISTS matricula CASCADE;

/*
select * 
INTO TABLE matricula
from (select 
	(case when fc.funtipocontrato not in (2,3,4) then fundataadmissao::varchar else null end) as database, --0		
	(case when fc.funtipocontrato not in (2,3,4) then (coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),regcodigo::varchar))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio','1'))))) else null end)::varchar as vinculoEmpregaticio,
	(case fc.regcodigo when 24 then 'true' when 20 then 'true' else 'false' end) as contratoTemporario, --2
	(case when fc.funtipocontrato not in (2,3,4) then 'NORMAL' else null end) as indicativoAdmissao,
	(case when fc.funtipocontrato not in (2,3,4) then 'URBANO' else null end) as naturezaAtividade,
	(case when fc.funtipocontrato not in (2,3,4) then (case funtipocontrato when 10 then 'TRANSFERENCIA' else 'ADMISSAO' end) else null end) as tipoAdmissao,	--5
	 (case when fc.funtipocontrato not in (2,3,4) then (case funtipoemprego when 1 then 'true' else  'false' end) else null end)as primeiroEmprego, --6
	 (case regcodigo when 1 then  'true' else	'false'	 end) as optanteFgts,
	fundataopcaofgts::varchar as dataOpcao,
	ifcsequenciafgts as contaFgts,
	unicodigocsi as sindicato,--10
	null as tipoProvimento,
	null as leiContrato,
	--txjcodigo as atoContrato,
	null as atoContrato,
	(case when fc.funtipocontrato not in (2) then fundatanomeacao::varchar else null end) as dataNomeacao,
	(case when fc.funtipocontrato not in (2) then fundataposse::varchar else null end) as dataPosse,--15
	null as tempoAposentadoria,
	--true as previdenciaFederal,	
	(case when fc.funtipocontrato not in (2,3,4) then (case when fc.funtipocontrato not in (2) then (case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1) when 1 then true when 6 then true else false end) else null end) else null end)::varchar as previdenciaFederal,
	(case when fc.funtipocontrato not in (2,3,4) then (case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1) when 1 then null when 6 then null else ((case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1) when 3 then 'ESTADUAL' when 4 then 'FUNDO_ASSISTENCIA' when 5 then 'FUNDO_FINANCEIRO' when 2 then 'FUNDO_PREVIDENCIA' else null end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'plano-previdencia', (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1))))) end) else null end)::varchar as previdencias,	
	(case when fc.funtipocontrato not in (4) then coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),carcodigo))),'0') else null end)::varchar as cargo,--19
	null as cargoAlterado,--20
	--txjcodigo as atoAlteracaoCargo,
	null as atoAlteracaoCargo,
	null as areaAtuacao,
	null as areaAtuacaoAlterada,
	null as motivoAlteracaoAreaAtuacao,
	(case when fc.funtipocontrato not in (4) then (case funocupavaga when 1 then true else false end) else null end)::varchar as ocupaVaga,--25
	null as salarioAlterado,
	null as origemSalario,	
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),nivcodigo)))::varchar as nivelSalarial,
	null as classeReferencia,
	null as cargoComissionado,--30
	null as areaAtuacaoComissionado,
	(case when fc.funtipocontrato not in (2,3,4) then false else null end)::varchar as ocupaVagaComissionado,
	null as salarioComissionado,
	null as nivelSalarialComissionado,
	null as classeReferenciaComissionado,--35
	(case when fc.funtipocontrato not in (2,3,4) then 'MENSALISTA' else null end)::varchar as unidadePagamento,
	(case funformapagamento when 2 then (case when (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'conta-bancaria', (select left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) from wun.tbunico as u where u.unicodigo = fc.unicodigo), (select ucb.ifcnumeroconta from wun.tbunicocontabanco as ucb where ucb.unicodigo = fc.unicodigo and ucb.ifcsequencia = fc.ifcsequenciapaga)))) > 1 then 'CREDITO_EM_CONTA' else null end)  when 3 then 'DINHEIRO' when 4 then 'CHEQUE' else 'DINHEIRO' end) as formaPagamento,
	--'DINHEIRO' as formaPagamento,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'conta-bancaria', (select left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) from wun.tbunico as u where u.unicodigo = fc.unicodigo), (select ucb.ifcnumeroconta from wun.tbunicocontabanco as ucb where ucb.unicodigo = fc.unicodigo and ucb.ifcsequencia = fc.ifcsequenciapaga)))) as contaBancariaPagamento,
	(case when fc.funtipocontrato not in (3,4) then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), 1))) else null end)::varchar as configuracaoFerias,
	cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer) as quantidadeHorasMes,--40
	(case when fc.funtipocontrato not in (2,3,4) then (cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer)/5) else null end)::varchar as quantidadeHorasSemana,
	(case when fc.funtipocontrato not in (2,3,4) then false else null end) as jornadaParcial,
	null as dataAgendamentoRescisao,
	null as funcoesGratificadas, 
	(case fc.funsituacao when 1 then null else (case fc.regcodigo when 24 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) when 20 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) else null end) end)::varchar as dataTerminoContratoTemporario,
	null as motivoContratoTemporario,
	null as tipoInclusaoContratoTemporario,
	null as dataProrrogacaoContratoTemporario,
	(case when fc.funtipocontrato not in (3,4) then funcartaoponto else null end)::varchar as numeroCartaoPonto,
	null as parametroPonto,--50
	null as indicativoProvimento,
	null as orgaoOrigem,
	null as matriculaEmpresaOrigem,
	null as dataAdmissaoOrigem,
	(case when fc.funtipocontrato not in (3,4) then (case fc.funnocivos   when 0 then null   when 1 then 'NUNCA_EXPOSTO_AGENTES_NOCIVOS'  when 2 then 'EXPOSTO_APOSENTADORIA_15_ANOS'  when 5 then 'EXPOSTO_APOSENTADORIA_15_ANOS'     when 3 then 'EXPOSTO_APOSENTADORIA_20_ANOS'  when 7 then 'EXPOSTO_APOSENTADORIA_20_ANOS'   when 4 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    when 8 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    else     null    end) else null end)::varchar as ocorrenciaSefip,--55
	null as controleJornada,--56
	null as configuracaoLicencaPremio,
	null as configuracaoAdicional,
	null as processaAverbacao,
	(case when fc.funtipocontrato in (2) then coalesce(fc.fundatatermcont::varchar,'2021-01-01') else null  end)::varchar as dataFinal,--60
	null as dataProrrogacao,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro cmr where tipo_registro = 'pessoa-juridica' and id_gerado is not null limit 1) else null end)::varchar as instituicaoEnsino,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro cmr where tipo_registro = 'pessoa-juridica' and id_gerado is not null limit 1) else null end)::varchar as agenteIntegracao,
	(case when fc.funtipocontrato in (2) then '1363' else null end)::varchar as formacao,
	null as formacaoPeriodo,--65
	null as formacaoFase,
	null as estagioObrigatorio,
	null as objetivo,
	(case when fc.funtipocontrato not in (3,4) then fc.funcontrato else null end)::varchar as numeroContrato,
	null as possuiSeguroVida,--70
	null as numeroApoliceSeguroVida,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'categoria-trabalhador', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),'Estagiário', '901'))) else null end)::varchar as categoriaTrabalhador,
	--null as responsaveis,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro cmr where tipo_registro = 'pessoa-fisica' and id_gerado is not null limit 1) || '%|%' || (SELECT to_date(fc.odomesano||'01','YYYYMMDD')::varchar) else null end)::varchar as responsaveis,
	null as dataCessacaoAposentadoria,
	(case when fc.funtipocontrato in (3,4) then 5924 else null end)::varchar as entidadeOrigem,--75
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
	(case when fc.funtipocontrato in (9,8) then 'CEDIDO' else (CASE fc.funsituacao WHEN  1 THEN 'TRABALHANDO'  WHEN 2 THEN 'AFASTADO' ELSE 'DEMITIDO' end) end) as situacao,
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
	coalesce((select (case when nivsalariobase < 1 then null else nivsalariobase end) from wfp.tbnivel n where n.nivcodigo = fc.nivcodigo and n.odomesano = fc.odomesano),(case when funsalariobase < 1 then null else funsalariobase end),1.0) as rendimentoMensal,--94
	--(select txjcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc  limit 1) as atoAlteracaoSalario,
	null as atoAlteracaoSalario,--95
	--(select mtrcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc limit 1)  as motivoAlteracaoSalario,
	null as motivoAlteracaoSalario, 
	null as validationStatus,		
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','configuracao-organograma',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), 1)))::varchar,(select left((c.organo::varchar || regexp_replace(c.cncclassif, '[\.]', '', 'gi') || '000000000000000'),15) from wun.tbcencus as c where c.cnccodigo = fc.cnccodigo limit 1)::varchar))) as organograma,
	--select * from wfp.tblocaisresponsavel t 
	--((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'lotacao-fisica',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), right('000' || cast(fc.cltcodigo as text), 3)))) || '%|%' || 'true' || '%|%' || fc.data || '%|%' ||  '%|%' || ) as teste,
	null as lotacoesFisicas,--99
	null as historicos,--100
	row_number() over() as id,
	fc.fcncodigo,
	fc.funcontrato,
	fc.odomesano as competencia,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as clicodigo
from wfp.tbfuncontrato as fc  join wfp.tbfuncionario as f on f.fcncodigo = fc.fcncodigo and f.odomesano = fc.odomesano
) as s
where 
--(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', clicodigo, fcncodigo, funcontrato))) is null and
pessoa is not null;
*/

create index if not exists idx_m_fc on matricula (fcncodigo, funcontrato);

-- SELECT string_agg('coalesce(suc.' || column_name || '::varchar,'''')',' || ''%&%'' ||')  FROM information_schema.columns WHERE  table_name   = 'matricula';

select * 
from (select 	
	(case when fc.funtipocontrato not in (2,3,4) then fundataadmissao::varchar else null end) as database, --0		
	(case when fc.funtipocontrato not in (2,3,4) then (coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),regcodigo::varchar))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio','1'))))) else null end)::varchar as vinculoEmpregaticio,
	(case fc.regcodigo when 24 then 'true' when 20 then 'true' else 'false' end) as contratoTemporario, --2
	(case when fc.funtipocontrato not in (2,3,4) then 'NORMAL' else null end) as indicativoAdmissao,
	(case when fc.funtipocontrato not in (2,3,4) then 'URBANO' else null end) as naturezaAtividade,
	(case when fc.funtipocontrato not in (2,3,4) then (case funtipocontrato when 10 then 'TRANSFERENCIA' else 'ADMISSAO' end) else null end) as tipoAdmissao,	--5
	 (case when fc.funtipocontrato not in (2,3,4) then (case funtipoemprego when 1 then 'true' else  'false' end) else null end)as primeiroEmprego, --6
	 (case regcodigo when 1 then  'true' else	'false'	 end) as optanteFgts,
	fundataopcaofgts::varchar as dataOpcao,
	ifcsequenciafgts as contaFgts,
	unicodigocsi as sindicato,--10
	null as tipoProvimento,
	null as leiContrato,
	--txjcodigo as atoContrato,
	null as atoContrato,
	(case when fc.funtipocontrato not in (2) then fundatanomeacao::varchar else null end) as dataNomeacao,
	(case when fc.funtipocontrato not in (2) then fundataposse::varchar else null end) as dataPosse,--15
	null as tempoAposentadoria,
	--true as previdenciaFederal,	
	(case when fc.funtipocontrato not in (2,3,4) then (case when fc.funtipocontrato not in (2) then (case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1) when 1 then true when 6 then true else false end) else null end) else null end)::varchar as previdenciaFederal,
	(case when fc.funtipocontrato not in (2,3,4) then (case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = fc.funcontrato and p.fcncodigo = fc.fcncodigo and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1) when 1 then null when 6 then null else ((case (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1) when 3 then 'ESTADUAL' when 4 then 'FUNDO_ASSISTENCIA' when 5 then 'FUNDO_FINANCEIRO' when 2 then 'FUNDO_PREVIDENCIA' else null end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'plano-previdencia', (select p.tpvcodigo from wfp.tbfunpreviden as p where p.fcncodigo = fc.fcncodigo and p.funcontrato = fc.funcontrato and p.odomesano = fc.odomesano and p.fprprincipal = 1 limit 1))))) end) else null end)::varchar as previdencias,	
	(case when fc.funtipocontrato not in (4) then coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),carcodigo))),'0') else null end)::varchar as cargo,--19
	null as cargoAlterado,--20
	--txjcodigo as atoAlteracaoCargo,
	null as atoAlteracaoCargo,
	null as areaAtuacao,
	null as areaAtuacaoAlterada,
	null as motivoAlteracaoAreaAtuacao,
	(case when fc.funtipocontrato not in (4) then (case funocupavaga when 1 then true else false end) else null end)::varchar as ocupaVaga,--25
	null as salarioAlterado,
	null as origemSalario,	
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),nivcodigo)))::varchar as nivelSalarial,
	null as classeReferencia,
	null as cargoComissionado,--30
	null as areaAtuacaoComissionado,
	(case when fc.funtipocontrato not in (2,3,4) then false else null end)::varchar as ocupaVagaComissionado,
	null as salarioComissionado,
	null as nivelSalarialComissionado,
	null as classeReferenciaComissionado,--35
	(case when fc.funtipocontrato not in (2,3,4) then 'MENSALISTA' else null end)::varchar as unidadePagamento,
	(case funformapagamento when 2 then (case when (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'conta-bancaria', (select left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) from wun.tbunico as u where u.unicodigo = fc.unicodigo), (select ucb.ifcnumeroconta from wun.tbunicocontabanco as ucb where ucb.unicodigo = fc.unicodigo and ucb.ifcsequencia = fc.ifcsequenciapaga)))) > 1 then 'CREDITO_EM_CONTA' else null end)  when 3 then 'DINHEIRO' when 4 then 'CHEQUE' else 'DINHEIRO' end) as formaPagamento,
	--'DINHEIRO' as formaPagamento,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'conta-bancaria', (select left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) from wun.tbunico as u where u.unicodigo = fc.unicodigo), (select ucb.ifcnumeroconta from wun.tbunicocontabanco as ucb where ucb.unicodigo = fc.unicodigo and ucb.ifcsequencia = fc.ifcsequenciapaga)))) as contaBancariaPagamento,
	(case when fc.funtipocontrato not in (3,4) then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), 1))) else null end)::varchar as configuracaoFerias,
	cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer) as quantidadeHorasMes,--40
	(case when fc.funtipocontrato not in (2,3,4) then (cast(regexp_replace(funhorastrabmes, '\:\d{2}$', '', 'gi') as integer)/5) else null end)::varchar as quantidadeHorasSemana,
	(case when fc.funtipocontrato not in (2,3,4) then false else null end) as jornadaParcial,
	null as dataAgendamentoRescisao,
	null as funcoesGratificadas, 
	(case fc.funsituacao when 1 then null else (case fc.regcodigo when 24 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) when 20 then (select resdata from wfp.tbrescisaocalculada as rc where rc.fcncodigo = fc.fcncodigo and rc.funcontrato = fc.funcontrato order by resdata desc limit 1) else null end) end)::varchar as dataTerminoContratoTemporario,
	null as motivoContratoTemporario,
	null as tipoInclusaoContratoTemporario,
	null as dataProrrogacaoContratoTemporario,
	(case when fc.funtipocontrato not in (3,4) then funcartaoponto else null end)::varchar as numeroCartaoPonto,
	null as parametroPonto,--50
	null as indicativoProvimento,
	null as orgaoOrigem,
	null as matriculaEmpresaOrigem,
	null as dataAdmissaoOrigem,
	(case when fc.funtipocontrato not in (3,4) then (case fc.funnocivos   when 0 then null   when 1 then 'NUNCA_EXPOSTO_AGENTES_NOCIVOS'  when 2 then 'EXPOSTO_APOSENTADORIA_15_ANOS'  when 5 then 'EXPOSTO_APOSENTADORIA_15_ANOS'     when 3 then 'EXPOSTO_APOSENTADORIA_20_ANOS'  when 7 then 'EXPOSTO_APOSENTADORIA_20_ANOS'   when 4 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    when 8 then 'EXPOSTO_APOSENTADORIA_25_ANOS'    else     null    end) else null end)::varchar as ocorrenciaSefip,--55
	null as controleJornada,--56
	null as configuracaoLicencaPremio,
	null as configuracaoAdicional,
	null as processaAverbacao,
	(case when fc.funtipocontrato in (2) then coalesce(fc.fundatatermcont::varchar,'2021-01-01') else null  end)::varchar as dataFinal,--60
	null as dataProrrogacao,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro cmr where tipo_registro = 'pessoa-juridica' and id_gerado is not null limit 1) else null end)::varchar as instituicaoEnsino,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro cmr where tipo_registro = 'pessoa-juridica' and id_gerado is not null limit 1) else null end)::varchar as agenteIntegracao,	
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'formacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), (case (select uf.gincodigo from wun.tbunicofisica as uf where uf.unicodigo = fc.unicodigo) 	when 2 then 10	when 3 then 1	when 4 then 1	when 12 then 1	when 13 then 1	when 14 then 1	when 5 then 2	when 6 then 2	when 7 then 3	when 8 then 3	when 16 then 3	when 9 then 4	when 10 then 8	when 11 then 9	when 17 then 9	else 10 end)))) as formacao,
	null as formacaoPeriodo,--65
	null as formacaoFase,
	null as estagioObrigatorio,
	null as objetivo,
	(case when fc.funtipocontrato not in (3,4) then fc.funcontrato else null end)::varchar as numeroContrato,
	null as possuiSeguroVida,--70
	null as numeroApoliceSeguroVida,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'categoria-trabalhador', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),'Estagiário', '901'))) else null end)::varchar as categoriaTrabalhador,
	--null as responsaveis,
	(case when fc.funtipocontrato in (2) then (select id_gerado from public.controle_migracao_registro cmr where tipo_registro = 'pessoa-fisica' and id_gerado is not null limit 1) || '%|%' || (SELECT to_date(fc.odomesano||'01','YYYYMMDD')::varchar) else null end)::varchar as responsaveis,
	null as dataCessacaoAposentadoria,
	(case when fc.funtipocontrato in (3,4) then 5924 else null end)::varchar as entidadeOrigem,--75
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
	(case when fc.funtipocontrato in (9,8) then 'CEDIDO' else (CASE fc.funsituacao WHEN  1 THEN 'TRABALHANDO'  WHEN 2 THEN 'AFASTADO' ELSE 'DEMITIDO' end) end) as situacao,
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
	coalesce((select (case when nivsalariobase < 1 then null else nivsalariobase end) from wfp.tbnivel n where n.nivcodigo = fc.nivcodigo and n.odomesano = fc.odomesano),(case when funsalariobase < 1 then null else funsalariobase end),1.0) as rendimentoMensal,--94
	--(select txjcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc  limit 1) as atoAlteracaoSalario,
	null as atoAlteracaoSalario,--95
	--(select mtrcodigo from wfp.tbfunhistoricosalarial as fhs where fhs.fcncodigo = fc.fcncodigo and fhs.odomesano = fc.odomesano order by fhsdatahora desc limit 1)  as motivoAlteracaoSalario,
	null as motivoAlteracaoSalario, 
	null as validationStatus,		
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','configuracao-organograma',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), 1)))::varchar,(select left((c.organo::varchar || regexp_replace(c.cncclassif, '[\.]', '', 'gi') || '000000000000000'),15) from wun.tbcencus as c where c.cnccodigo = fc.cnccodigo limit 1)::varchar))) as organograma,
	--select * from wfp.tblocaisresponsavel t 
	--((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'lotacao-fisica',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), right('000' || cast(fc.cltcodigo as text), 3)))) || '%|%' || 'true' || '%|%' || fc.data || '%|%' ||  '%|%' || ) as teste,
	null as lotacoesFisicas,--99
	/**/(select 	
	string_agg( 
	coalesce(suc.database::varchar,'') || '%&%' ||coalesce(suc.vinculoempregaticio::varchar,'') || '%&%' ||coalesce(suc.contratotemporario::varchar,'') || '%&%' ||coalesce(suc.indicativoadmissao::varchar,'') || '%&%' ||coalesce(suc.naturezaatividade::varchar,'') || '%&%' ||coalesce(suc.tipoadmissao::varchar,'') || '%&%' ||coalesce(suc.primeiroemprego::varchar,'') || '%&%' ||coalesce(suc.optantefgts::varchar,'') || '%&%' ||coalesce(suc.dataopcao::varchar,'') || '%&%' ||coalesce(suc.contafgts::varchar,'') || '%&%' ||coalesce(suc.sindicato::varchar,'') || '%&%' ||coalesce(suc.tipoprovimento::varchar,'') || '%&%' ||coalesce(suc.leicontrato::varchar,'') || '%&%' ||coalesce(suc.atocontrato::varchar,'') || '%&%' ||coalesce(suc.datanomeacao::varchar,'') || '%&%' ||coalesce(suc.dataposse::varchar,'') || '%&%' ||coalesce(suc.tempoaposentadoria::varchar,'') || '%&%' ||coalesce(suc.previdenciafederal::varchar,'') || '%&%' ||coalesce(suc.previdencias::varchar,'') || '%&%' ||coalesce(suc.cargo::varchar,'') || '%&%' ||coalesce(suc.cargoalterado::varchar,'') || '%&%' ||coalesce(suc.atoalteracaocargo::varchar,'') || '%&%' ||coalesce(suc.areaatuacao::varchar,'') || '%&%' ||coalesce(suc.areaatuacaoalterada::varchar,'') || '%&%' ||coalesce(suc.motivoalteracaoareaatuacao::varchar,'') || '%&%' ||coalesce(suc.ocupavaga::varchar,'') || '%&%' ||coalesce(suc.salarioalterado::varchar,'') || '%&%' ||coalesce(suc.origemsalario::varchar,'') || '%&%' ||coalesce(suc.nivelsalarial::varchar,'') || '%&%' ||coalesce(suc.classereferencia::varchar,'') || '%&%' ||coalesce(suc.cargocomissionado::varchar,'') || '%&%' ||coalesce(suc.areaatuacaocomissionado::varchar,'') || '%&%' ||coalesce(suc.ocupavagacomissionado::varchar,'') || '%&%' ||coalesce(suc.salariocomissionado::varchar,'') || '%&%' ||coalesce(suc.nivelsalarialcomissionado::varchar,'') || '%&%' ||coalesce(suc.classereferenciacomissionado::varchar,'') || '%&%' ||coalesce(suc.unidadepagamento::varchar,'') || '%&%' ||coalesce(suc.formapagamento::varchar,'') || '%&%' ||coalesce(suc.contabancariapagamento::varchar,'') || '%&%' ||coalesce(suc.configuracaoferias::varchar,'') || '%&%' ||coalesce(suc.quantidadehorasmes::varchar,'') || '%&%' ||coalesce(suc.quantidadehorassemana::varchar,'') || '%&%' ||coalesce(suc.jornadaparcial::varchar,'') || '%&%' ||coalesce(suc.dataagendamentorescisao::varchar,'') || '%&%' ||coalesce(suc.funcoesgratificadas::varchar,'') || '%&%' ||coalesce(suc.dataterminocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.motivocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.tipoinclusaocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.dataprorrogacaocontratotemporario::varchar,'') || '%&%' ||coalesce(suc.numerocartaoponto::varchar,'') || '%&%' ||coalesce(suc.parametroponto::varchar,'') || '%&%' ||coalesce(suc.indicativoprovimento::varchar,'') || '%&%' ||coalesce(suc.orgaoorigem::varchar,'') || '%&%' ||coalesce(suc.matriculaempresaorigem::varchar,'') || '%&%' ||coalesce(suc.dataadmissaoorigem::varchar,'') || '%&%' ||coalesce(suc.ocorrenciasefip::varchar,'') || '%&%' ||coalesce(suc.controlejornada::varchar,'') || '%&%' ||coalesce(suc.configuracaolicencapremio::varchar,'') || '%&%' ||coalesce(suc.configuracaoadicional::varchar,'') || '%&%' ||coalesce(suc.processaaverbacao::varchar,'') || '%&%' ||coalesce(suc.datafinal::varchar,'') || '%&%' ||coalesce(suc.dataprorrogacao::varchar,'') || '%&%' ||coalesce(suc.instituicaoensino::varchar,'') || '%&%' ||coalesce(suc.agenteintegracao::varchar,'') || '%&%' ||coalesce(suc.formacao::varchar,'') || '%&%' ||coalesce(suc.formacaoperiodo::varchar,'') || '%&%' ||coalesce(suc.formacaofase::varchar,'') || '%&%' ||coalesce(suc.estagioobrigatorio::varchar,'') || '%&%' ||coalesce(suc.objetivo::varchar,'') || '%&%' ||coalesce(suc.numerocontrato::varchar,'') || '%&%' ||coalesce(suc.possuisegurovida::varchar,'') || '%&%' ||coalesce(suc.numeroapolicesegurovida::varchar,'') || '%&%' ||coalesce(suc.categoriatrabalhador::varchar,'') || '%&%' ||coalesce(suc.responsaveis::varchar,'') || '%&%' ||coalesce(suc.datacessacaoaposentadoria::varchar,'') || '%&%' ||coalesce(suc.entidadeorigem::varchar,'') || '%&%' ||coalesce(suc.motivoaposentadoria::varchar,'') || '%&%' ||coalesce(suc.funcionarioorigem::varchar,'') || '%&%' ||coalesce(suc.tipomovimentacao::varchar,'') || '%&%' ||coalesce(suc.motivoiniciobeneficio::varchar,'') || '%&%' ||coalesce(suc.duracaobeneficio::varchar,'') || '%&%' ||coalesce(suc.datacessacaobeneficio::varchar,'') || '%&%' ||coalesce(suc.motivocessacaobeneficio::varchar,'') || '%&%' ||coalesce(suc.matriculaorigem::varchar,'') || '%&%' ||coalesce(suc.responsavel::varchar,'') || '%&%' ||coalesce(suc.datainiciocontrato::varchar,'') || '%&%' ||coalesce(suc.situacao::varchar,'') || '%&%' ||coalesce(suc.iniciovigencia::varchar,'') || '%&%' ||coalesce(suc.tipo::varchar,'') || '%&%' ||coalesce(suc.pessoa::varchar,'') || '%&%' ||coalesce(suc.codigomatricula::varchar,'') || '%&%' ||coalesce(suc.esocial::varchar,'') || '%&%' ||coalesce(suc.grupofuncional::varchar,'') || '%&%' ||coalesce(suc.jornadatrabalho::varchar,'') || '%&%' ||coalesce(suc.rendimentomensal::varchar,'') || '%&%' ||coalesce(suc.atoalteracaosalario::varchar,'') || '%&%' ||coalesce(suc.motivoalteracaosalario::varchar,'') || '%&%' ||coalesce(suc.validationstatus::varchar,'') || '%&%' ||coalesce(suc.organograma::varchar,'') || '%&%' ||coalesce(suc.lotacoesfisicas::varchar,'') || '%&%' ||coalesce(suc.historicos::varchar,'') || '%&%' ||coalesce(suc.id::varchar,'') || '%&%' ||coalesce(suc.fcncodigo::varchar,'') || '%&%' ||coalesce(suc.funcontrato::varchar,'') || '%&%' ||coalesce(suc.competencia::varchar,'')
	,'%&&%')
	from ( select * from (select distinct on (m.vinculoempregaticio,m.contratotemporario,m.indicativoadmissao,m.naturezaatividade,m.tipoadmissao,m.primeiroemprego,m.optantefgts,m.dataopcao,m.contafgts,m.sindicato,m.tipoprovimento,m.leicontrato,m.atocontrato,m.datanomeacao,m.dataposse,m.tempoaposentadoria,m.previdenciafederal,m.previdencias,m.cargo,m.cargoalterado,m.atoalteracaocargo,m.areaatuacao,m.areaatuacaoalterada,m.motivoalteracaoareaatuacao,m.ocupavaga,m.salarioalterado,m.origemsalario,m.nivelsalarial,m.classereferencia,m.cargocomissionado,m.areaatuacaocomissionado,m.ocupavagacomissionado,m.salariocomissionado,m.nivelsalarialcomissionado,m.classereferenciacomissionado,m.unidadepagamento,m.formapagamento,m.contabancariapagamento,m.configuracaoferias,m.quantidadehorasmes,m.quantidadehorassemana,m.jornadaparcial,m.dataagendamentorescisao,m.funcoesgratificadas,m.dataterminocontratotemporario,m.motivocontratotemporario,m.tipoinclusaocontratotemporario,m.dataprorrogacaocontratotemporario,m.numerocartaoponto,m.parametroponto,m.indicativoprovimento,m.orgaoorigem,m.matriculaempresaorigem,m.dataadmissaoorigem,m.ocorrenciasefip,m.controlejornada,m.configuracaolicencapremio,m.configuracaoadicional,m.processaaverbacao,m.datafinal,m.dataprorrogacao,m.instituicaoensino,m.agenteintegracao,m.formacao,m.formacaoperiodo,m.formacaofase,m.estagioobrigatorio,m.objetivo,m.numerocontrato,m.possuisegurovida,m.numeroapolicesegurovida,m.categoriatrabalhador,m.responsaveis,m.datacessacaoaposentadoria,m.entidadeorigem,m.motivoaposentadoria,m.funcionarioorigem,m.tipomovimentacao,m.motivoiniciobeneficio,m.duracaobeneficio,m.datacessacaobeneficio,m.motivocessacaobeneficio,m.matriculaorigem,m.responsavel,m.situacao,m.tipo,m.pessoa,m.codigomatricula,m.esocial,m.grupofuncional,m.jornadatrabalho,m.rendimentomensal,m.atoalteracaosalario,m.motivoalteracaosalario,m.validationstatus,m.organograma,m.lotacoesfisicas,m.historicos) * from matricula as m where m.fcncodigo = fc.fcncodigo and m.funcontrato = fc.funcontrato and m.competencia < fc.odomesano) as sssuc  order by sssuc.competencia asc) as suc) as historicos,
	/**/row_number() over() as id,
	fc.fcncodigo,
	fc.funcontrato,	
	fc.odomesano as competencia,
	fc.funtipocontrato,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as clicodigo
	,(select uninomerazao from wun.tbunico as u where u.unicodigo = fc.unicodigo) as nome
from wfp.tbfuncontrato as fc  join wfp.tbfuncionario as f on f.fcncodigo = fc.fcncodigo and f.odomesano = fc.odomesano
where fc.odomesano = 202010
--and fc.funtipocontrato not in (4)
--and fc.fcncodigo = 15605
--and fc.fcncodigo = 603
and fc.fcncodigo in (1747,2279,14020,570,12684)
and fc.funsituacao in (1,2)
order by fc.fcncodigo,fc.funcontrato
--limit 10 offset 0
) as s
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', clicodigo, fcncodigo, funcontrato))) is null 
--and pessoa is not null;
