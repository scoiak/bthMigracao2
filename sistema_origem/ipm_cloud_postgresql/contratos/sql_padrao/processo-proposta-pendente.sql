select distinct
	0 as id,
	qcp.clicodigo,
	qcp.minano,
	qcp.minnro
from wco.tbcadqcp qcp
where qcp.clicodigo = {{clicodigo}}
and qcp.minano = {{ano}}
--and qcp.minnro not in (35, 41, 65, 81, 88, 90, 92, 99, 100)
and qcp.minnro = 119
order by 1, 2 desc, 3 desc