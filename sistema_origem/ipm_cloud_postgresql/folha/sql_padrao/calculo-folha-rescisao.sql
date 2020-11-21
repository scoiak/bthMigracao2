select * from ( select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc, dataRescisao asc) as codigo,
* from (
select
	'RESCISAO' AS tipoProcessamento,
	(case tipcodigo when 3 then 'INTEGRAL' when 9 then 'COMPLEMENTAR' else null end) AS subTipoProcessamento,
    NULL AS dataAgendamento,
    resdata::varchar AS dataPagamento,
	'INDIVIDUAL'  AS tipoVinculacaoMatricula,
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,	 
    NULL AS saldoFgts,
    false AS fgtsMesAnterior,
    --TRUE AS conversao,
    resdataaviso::varchar AS avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), motcodigo::varchar))) AS motivoRescisao,
    resdatarescisao::varchar AS dataRescisao,
    --NULL AS ato,
    false AS consideraAvosPerdidosDecimoTerceiro,
    true AS descontarFaltasFerias,
    FALSE AS trabalhouDiaRescisao,
    FALSE as reporVaga       
    from wfp.tbrescisaocalculada
--where fcncodigo = 56
union all
SELECT distinct
'RESCISAO' AS tipoProcessamento,
	(case tipcodigo when 3 then 'INTEGRAL' when 9 then 'COMPLEMENTAR' else null end) AS subTipoProcessamento,
    NULL AS dataAgendamento,
    pagdata::varchar AS dataPagamento,
	'INDIVIDUAL'  AS tipoVinculacaoMatricula,
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), p.fcncodigo, p.funcontrato))) as matricula,	 
    NULL AS saldoFgts,
    false AS fgtsMesAnterior,
    --TRUE AS conversao,
    null AS avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), '9'))) AS motivoRescisao,
    pagdata::varchar AS dataRescisao,
    --NULL AS ato,
    false AS consideraAvosPerdidosDecimoTerceiro,
    true AS descontarFaltasFerias,
    FALSE AS trabalhouDiaRescisao,
    FALSE as reporVaga       
	FROM wfp.tbpagamento as p
	where tipcodigo in (3,9)
	--and p.fcncodigo = 2161  	
) as a
) as b
where matricula is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,codigo))) is not null

--SELECT * FROM wfp.tbcalculofolha;
--select * from wfp.tbrescisaocalculada;