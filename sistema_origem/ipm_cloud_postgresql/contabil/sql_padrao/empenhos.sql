select
	e.clicodigo,
	e.loaano,
	e.empnro,
	e.empdataemissao,
	p.uninomerazao,
	p.unicpfcnpj,
	e.empvalor
from weo.tbempenho e
inner join wun.tbunico p on (e.unicodigo = p.unicodigo)
where e.clicodigo = 2016
and e.loaano = 2020
order by e.empnro desc
limit 30