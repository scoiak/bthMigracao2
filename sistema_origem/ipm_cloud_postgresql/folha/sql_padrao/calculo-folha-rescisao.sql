--SELECT * FROM wfp.tbcalculofolha;
--select * from wfp.tbrescisaocalculada;
--COMPLEMENTAR NAO ESTA ENVIANDO
select * from ( select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))) as entidade,
row_number() over(partition by matricula order by matricula asc, dataPagamento asc) as codigo,
* from (
select distinct on (matricula,dataPagamento) * from (
	SELECT distinct
	'RESCISAO' AS tipoProcessamento,
	'PAGAMENTO' as tipoRescisao,
	(case tipcodigo when 3 then 'INTEGRAL' when 9 then 'COMPLEMENTAR' else null end) AS subTipoProcessamento,
    NULL AS dataAgendamento,
    pagdata::varchar AS dataPagamento,
	'INDIVIDUAL'  AS tipoVinculacaoMatricula,
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))), p.fcncodigo, p.funcontrato))) as matricula,	 
    NULL AS saldoFgts,
    false AS fgtsMesAnterior,
    --TRUE AS conversao,
    null AS avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))), '9'))) AS motivoRescisao,
    pagdata::varchar AS dataRescisao,
    --NULL AS ato,
    false AS consideraAvosPerdidosDecimoTerceiro,
    true AS descontarFaltasFerias,
    FALSE AS trabalhouDiaRescisao,
    FALSE as reporVaga       
	FROM wfp.tbpagamento as p
	where tipcodigo in (3,9)	
	-- and fcncodigo in (16273,17595,17723)--(4714,2,113,15011,56,10438,11166,15079,15200)		
	union all
	select 
	'RESCISAO' AS tipoProcessamento,
	'RESCISAO' as tipoRescisao,
	(case tipcodigo when 3 then 'INTEGRAL' when 9 then 'COMPLEMENTAR' else null end) AS subTipoProcessamento,
    NULL AS dataAgendamento,
    resdata::varchar AS dataPagamento,
	'INDIVIDUAL'  AS tipoVinculacaoMatricula,
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))), fcncodigo, funcontrato))) as matricula,	 
    NULL AS saldoFgts,
    false AS fgtsMesAnterior,
    --TRUE AS conversao,
    resdataaviso::varchar AS avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))), motcodigo::varchar))) AS motivoRescisao,
    resdatarescisao::varchar AS dataRescisao,
    --NULL AS ato,
    false AS consideraAvosPerdidosDecimoTerceiro,
    true AS descontarFaltasFerias,
    FALSE AS trabalhouDiaRescisao,
    FALSE as reporVaga       
    from wfp.tbrescisaocalculada
	-- where fcncodigo in (16273,17595,17723)--(4714,2,113,15011,56,10438,11166,15079,15200)
) as s
) as a
) as b
where matricula is not null
--and matricula = 2614788
--and datapagamento::date >= '2020-01-01'::date	
--and matricula not in (2508376,2553267) --reintegracao
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'calculo-folha-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))),matricula,tipoProcessamento,subTipoProcessamento,dataPagamento))) is null
