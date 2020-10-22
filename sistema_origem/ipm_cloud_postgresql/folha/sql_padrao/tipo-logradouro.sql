select * from (
select
    tl.tplcodigo as id,
    tl.tplcodigo as codigo,
    substring(tl.tplnome,1,20) as descricao,
    (case when tl.tplnomeabreviado is null then cast(tplcodigo as varchar) else (case when (select tplnomeabreviado from wun.tbtipolograd where tplcodigo <> tl.tplcodigo order by tplcodigo limit 1) is not null then cast(tplcodigo as varchar) else tplnomeabreviado end) end) as abreviatura,
    public.bth_get_situacao_registro('300', 'tipo-logradouro', substring(upper(tl.tplnome),1,20)) as situacaoRegistro
from
    wun.tbtipolograd as tl
    ) as a
where situacaoRegistro in (0)