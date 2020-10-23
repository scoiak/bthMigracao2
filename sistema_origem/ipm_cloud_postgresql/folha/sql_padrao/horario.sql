select
	row_number() OVER () as id,
	row_number() OVER () as codigo,
	(hopentrada || ' até ' || hopsaida || ' [' || row_number() OVER () || ']') as descricao,
	hopentrada as entrada,
	hopsaida as entrada,
	'2020-01' as inicioVigencia,
	'false' as flexivel
from
	wfp.tbhorarioperiodo
where
	odomesano = '202009'