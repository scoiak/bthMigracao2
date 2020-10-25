select distinct
	'300' AS sistema,
	'horario' as tipo_registro,
	clicodigo as chave_dsk1,
	horcodigo as chave_dsk2,
	*
from (
	select
	   horcodigo as id,
	   clicodigo,
	   horcodigo,
	   COALESCE((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','horario', cast (clicodigo as text), cast(horcodigo as text)))), 0) as id_gerado,
	   hordescricao,
	   '08:00:00' as horaMinimaAlocacao,
	   'PT08H00M' as toleranciaAlocacao,
	   hordia,
	   '17:00:00' saida,
	   'PT0S' as toleranciaAnteriorEntrada,
	   'PT0S' as toleranciaPosteriorEntrada,
	   'PT0S' as toleranciaAnteriorSaida,
	   'PT0S' as toleranciaPosteriorSaida,
	   '2020-01-01T00:00:00.000Z' as inicioVigencia,
	   'false' as flexivel,
	   null as cargaHoraria
	from wfp.tbhorario
	where hordia is not null
) tab
where id_gerado = 0