select * from (
select
	b.baicodigo as id,
	b.baicodigo as codigo,
	substring(b.bainome,1,50) as nome,
    -- public.bth_get_id_gerado('300', 'cidades', (select c.cidnome from wun.tbcidade as c where c.cidcodigo = b.cidcodigo limit 1)) as municipio,
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','municipio', (select c.cidnome from wun.tbcidade as c where c.cidcodigo = b.cidcodigo limit 1)))) as municipio,
    null as zonaRural
from
	wun.tbbairro as b
) as a
where public.bth_get_situacao_registro('300', 'bairro', substring(upper(nome),1,50), cast(municipio as varchar)) in (0)
and municipio is not null