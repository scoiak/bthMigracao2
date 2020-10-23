select * from (
select
	l.logcodigo as id,
	l.logcodigo as codigo,
	l.lognome as descricao,
	null as cep,
	-- public.bth_get_id_gerado('300', 'tipo-logradouro', (select substring(upper(tl.tplnome),1,20) from wun.tbtipolograd as tl where tl.tplcodigo = l.tplcodigo limit 1)) as tipoLogradouro,
	-- public.bth_get_id_gerado('300', 'cidades', (select c.cidnome from wun.tbcidade as c where c.cidcodigo = l.cidcodigo limit 1)) as municipio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-logradouro', (select substring(upper(tl.tplnome),1,20) from wun.tbtipolograd as tl where tl.tplcodigo = l.tplcodigo limit 1))))as tipoLogradouro,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','cidades', (select c.cidnome from wun.tbcidade as c where c.cidcodigo = l.cidcodigo limit 1)))) as municipio,
	public.bth_get_situacao_registro('300', 'logradouro', cast(logcodigo as varchar)) as situacaoRegistro
from wun.tblogradouro as l
limit 10
) as a
where situacaoRegistro in (0)
and municipio is not null
and tipoLogradouro is not null