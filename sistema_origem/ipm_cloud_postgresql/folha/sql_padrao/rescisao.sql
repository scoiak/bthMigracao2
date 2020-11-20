select * from (select
	rctdatarescisao as data,
	null as saldoFgts,
	null as fgtsMesAnterior,
	true as conversao,
	null as ato,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	null as avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), motcodigo::varchar))) as motivoRescisao--(select ma.padcodigo from wfp.tbmotivoafasta as ma where ma.motcodigo = r.motcodigo limit 1)::varchar))) as motivoRescisao	
from 
	wfp.tbrescisaocontrato as r
where odomesano = 202010
and not exists (select fc.funsituacao from wfp.tbfuncontrato as fc where fc.funcontrato = r.funcontrato and fc.fcncodigo = r.fcncodigo and fc.odomesano = r.odomesano and fc.funsituacao = 1) 
) as a
--where matricula is not null
