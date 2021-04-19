select distinct
	0 as id,
	qcp.clicodigo,
	qcp.minano,
	qcp.minnro
from wco.tbcadqcp qcp
where qcp.clicodigo = {{clicodigo}}
and qcp.minano = {{ano}}
and qcp.minnro in (81)
--and qcp.minnro = 27
order by 1, 2 desc, 3 desc