select * from
(
select
	b.baicodigo as id,
	b.baicodigo as codigo,
	substring(b.bainome,1,50) as nome,
    public.bth_get_id_gerado('300', 'cidades', (select c.cidnome from wun.tbcidade as c where c.cidcodigo = b.cidcodigo limit 1)) as municipio,
    null as zonaRural,
    public.bth_get_situacao_registro('300', 'bairro', substring(upper(b.bainome),1,50)) as situacaoRegistro
from
	wun.tbbairro as b
) as a
where situacaoRegistro in (0)
and municipio is not null