select * from ( select
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc, data asc) as codigo,
* from (
select distinct on (matricula) * from (
SELECT distinct
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	pagdata::varchar as data,
	null as saldoFgts,
	false as fgtsMesAnterior,
	--true as conversao,
	null as ato,
	null as avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), '9'))) as motivoRescisao--(select ma.padcodigo from wfp.tbmotivoafasta as ma where ma.motcodigo = r.motcodigo limit 1)::varchar))) as motivoRescisao
	FROM wfp.tbpagamento as p
	where tipcodigo in (3,9)
	--and fcncodigo in (4714,2,113,15011,56)
	union all
select
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	rctdatarescisao::varchar as data,
	null as saldoFgts,
	false as fgtsMesAnterior,
	--true as conversao,
	null as ato,
	null as avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), motcodigo::varchar))) as motivoRescisao--(select ma.padcodigo from wfp.tbmotivoafasta as ma where ma.motcodigo = r.motcodigo limit 1)::varchar))) as motivoRescisao
from
	wfp.tbrescisaocontrato as r
where odomesano = 202011
and not exists (select fc.funsituacao from wfp.tbfuncontrato as fc where fc.funcontrato = r.funcontrato and fc.fcncodigo = r.fcncodigo and fc.odomesano = r.odomesano and fc.funsituacao = 1)
--and fcncodigo in (4714,2,113,15011,56)
) as s
	) as a
) as b
where matricula is not null
and motivorescisao is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,data))) is null