--create index if not exists idx_fg_pf on wfp.tbferiasgozada  (ferdatainicio, odomesano, fcncodigo);

select * from ( select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc, dataInicial asc) as codigo,
* from (
select 
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), 1))) as configuracaoFerias,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	(case when ferperdeudireito = 1 then 'CANCELADO' else (case ferpaga when 1 then 'EM_ANDAMENTO' when 2 then 'QUITADO' when 3 then 'CANCELADO' else null end) end) as situacao,
	pf.ferdatainicio::varchar as dataInicial,
	pf.ferdatafinal::varchar as dataFinal,
	(select substring(fg.fgomesanopagto::varchar,1,4) || '-' || substring(fg.fgomesanopagto::varchar,5,2) from wfp.tbferiasgozada as fg where fg.ferdatainicio = pf.ferdatainicio and fg.odomesano = pf.odomesano and fg.fcncodigo = pf.fcncodigo limit 1)::varchar as competenciaFechamentoProvisao,
	coalesce(ferdiasperda,0) as faltas,
	coalesce((case when ferpaga = 3 then 0 else ferdiasdireito - coalesce(ferdiasperda,0) end),0) as diasAdquiridos,
	coalesce((case when ferpaga = 3 or ferperdeudireito = 1 then ferdiasdireito - coalesce(ferdiasperda,0) else coalesce(ferdiasperda,0) end),0) as cancelados,
	0 as suspensos,
	coalesce((case ferpaga when 1 then ferdiasdireito when 2 then 0 when 3 then 0 else null end), 0) as saldo,
	null as pagou1TercoIntegral,
	--true as conversao,
	null as diasAnuladosRescisao,	
	(select string_agg('Concessao de Ferias'|| '%|%' || suc.fgodatainicio || '%|%' || suc.fgodatafinal || '%|%' || '' || '%|%' || '' || '%|%' || suc.fgodatainicio || '%|%' || suc.fgodatafinal || '%|%' || suc.fgodatainicio|| '%|%' || '' || '%|%' || suc.fgodiasgozo || '%|%' || '' || '%|%' || '' || '%|%' || '' || '%|%' || 'CONCESSAO' || '%|%' || suc.fgodatainicio || ' 00:00:00'  || '%|%' || suc.fgodiasgozo || '%|%' || '','%||%') from (select * from wfp.tbferiasgozada as fg where fg.ferdatainicio = pf.ferdatainicio and fg.odomesano = pf.odomesano and fg.fcncodigo = pf.fcncodigo) as suc) as movimentacoes
from 
	wfp.tbperiodoferia as pf
where 
	pf.odomesano = 202011
--and ferpaga not in (1)
--	and fcncodigo = 126
--	and fcncodigo = 266
--and fcncodigo in (56, 2 ,7959, 10438, 4714)
--and fcncodigo = 4714
) as a
) as b
where matricula is not null
--and matricula in (select id_gerado from controle_migracao_registro where tipo_registro = 'matricula' and i_chave_dsk2 in ('7959'))
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'periodo-aquisitivo-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,dataInicial))) is null
--limit 1000