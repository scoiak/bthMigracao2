select
	rctdatarescisao as data,
	null as saldoFgts,
	null as fgtsMesAnterior,
	null as conversao,
	null as ato,
	fcncodigo as matricula,
	null as avisoPrevio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao', motcodigo::varchar))) as motivoRescisao--(select ma.padcodigo from wfp.tbmotivoafasta as ma where ma.motcodigo = r.motcodigo limit 1)::varchar))) as motivoRescisao
from 
	wfp.tbrescisaocontrato as r