create index if not exists idx_p_fg on wfp.tbferiasgozada  (fcncodigo, funcontrato, fgodatainicio);
create index if not exists idx_p_pf on wfp.tbperiodoferia  (fcncodigo, funcontrato, odomesano, ferdatainicio);

select * from ( select
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'periodo-aquisitivo-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,datainicial))),0) as periodos,
row_number() over(partition by matricula order by matricula asc, dataPagamento asc) as codigo,
* from (
	select distinct
	'FERIAS' AS tipoProcessamento,
	'INTEGRAL' AS subTipoProcessamento,
	null as dataAgendamento,
	pagdata::varchar as dataPagamento,
	'INDIVIDUAL' tipoVinculacaoMatricula,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	true as consideraAvosPerdidos,
	--true as conversao,
	null as saldoFgts,
	30 as diasGozo,
	30 as saldo,
	false as fgtsMesAnterior,
	false as coletiva,
	true as pagarUmTercoIntegral,
	false as pagarDecimoTerceiroSalario,
	true as descontarFaltas,
	null as ato,
	null as anoDecimoTerceiro,
	true as consideraAvosPerdidosDecimoTerceiro,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-afastamento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),44)))::varchar as tipoAfastamento,
	(select fg.ferdatainicio from wfp.tbferiasgozada as fg where fg.odomesano = 202010 and fg.fcncodigo = p.fcncodigo and fg.funcontrato = p.funcontrato and substring(pagdata::varchar,1,7) between substring(fg.fgodatainicio::varchar,1,7) and substring(fg.fgodatafinal::varchar,1,7) limit 1)::varchar as dataInicial,
	(select pf.ferdatafinal from wfp.tbperiodoferia as pf where pf.fcncodigo = p.fcncodigo and pf.funcontrato = p.funcontrato and pf.odomesano = p.odomesano and pf.ferdatainicio = (select fg.ferdatainicio from wfp.tbferiasgozada as fg where fg.odomesano = 202010 and fg.fcncodigo = p.fcncodigo and fg.funcontrato = p.funcontrato and substring(pagdata::varchar,1,7) between substring(fg.fgodatainicio::varchar,1,7) and substring(fg.fgodatafinal::varchar,1,7) limit 1))::varchar as dataFinal,
	--substring(odomesano::varchar,1,4) || '-' || substring(odomesano::varchar,5,2) as competencia
	substring(pagdata::varchar,1,7) as competencia
	FROM wfp.tbpagamento as p
	where (tipcodigo in (2) or (tipcodigo in (1) and cpdcodigo in (77)))
    --and fcncodigo in (56, 2 ,7959, 10438, 4714)
) as a
) as b
where matricula is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'calculo-folha-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,tipoProcessamento,subTipoProcessamento,dataPagamento))) is null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'periodo-aquisitivo-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,datainicial))) is not null