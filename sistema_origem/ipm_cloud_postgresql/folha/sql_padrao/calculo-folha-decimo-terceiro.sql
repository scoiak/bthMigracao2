select * from ( select
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc, dataPagamento asc) as codigo,
* from (
	select distinct
	'DECIMO_TERCEIRO_SALARIO' AS tipoProcessamento,
	(case tipcodigo  when 6 then 'INTEGRAL' when 4 then 'INTEGRAL' when 5 then 'ADIANTAMENTO' else null end) AS subTipoProcessamento,
	 null as dataAgendamento,
	 pagdata::varchar as dataPagamento,
	 'TEMPORAL' tipoVinculacaoMatricula,--
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	 true as consideraAvosPerdidos,
	 null as saldoFgts,
	 false as fgtsMesAnterior,
	 substring(odomesano::varchar,1,4) as anoExercicio
	 --substring(odomesano::varchar,1,4) || '-' || substring(odomesano::varchar,5,2) as competencia
	FROM wfp.tbpagamento
	where tipcodigo in (4,5,6)
--
--and fcncodigo in (4714,2,113,15011,56,10438)
and odomesano >= 202001
) as a
) as b
where matricula is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'calculo-folha-decimo-terceiro',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,tipoProcessamento,subTipoProcessamento,dataPagamento))) is null
