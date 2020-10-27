select * from (
select
	l.logcodigo as id,
	l.logcodigo as codigo,
	left(upper(l.lognome),50) as descricao,
	null as cep,
	-- public.bth_get_id_gerado('300', 'tipo-logradouro', (select substring(upper(tl.tplnome),1,20) from wun.tbtipolograd as tl where tl.tplcodigo = l.tplcodigo limit 1)) as tipoLogradouro,
	-- public.bth_get_id_gerado('300', 'cidades', (select c.cidnome from wun.tbcidade as c where c.cidcodigo = l.cidcodigo limit 1)) as municipio,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-logradouro', (select left(upper(tl.tplnome),20) from wun.tbtipolograd as tl where tl.tplcodigo = l.tplcodigo limit 1)))) as tipoLogradouro,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','municipio', (select left(c.cidnome,50) from wun.tbcidade as c where c.cidcodigo = l.cidcodigo limit 1),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = l.cidcodigo limit 1) limit 1))))))) as municipio
from wun.tblogradouro as l
) as a
-- where public.bth_get_situacao_registro('300', 'logradouro', upper(descricao),cast(municipio as varchar)) in (0) and
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'logradouro', descricao, cast(municipio as varchar)))) is null
and municipio is not null
and tipoLogradouro is not null