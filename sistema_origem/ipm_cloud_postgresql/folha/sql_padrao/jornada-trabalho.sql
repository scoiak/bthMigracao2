select
	row_number() OVER () as id,
	row_number() OVER () as codigo,
	null as horario,
public.bth_get_id_gerado('300' , 'horario' , cast (horcodigo as text) , cast (horcodigo as text)) as id,
hopentrada || ' - ' || hopsaida as descricao,
'SEMANAL' as tipo,
timestamp '1900-01-01 00:00:00' as inicioVigencia
from wfp.tbhorario
where odomesano = '202009' -- última competência disponível

select * from wfp.tbhorarioperiodo
select * from wfp.tbhorario