select * from (
	select
	    tl.tplcodigo as id,
	    tl.tplcodigo as codigo,
	    left(upper(tl.tplnome),20) as descricao,
	    (case when tl.tplnomeabreviado is null then cast(tplcodigo as varchar) else (case when (select tplnomeabreviado from wun.tbtipolograd where tplcodigo <> tl.tplcodigo order by tplcodigo limit 1) is not null then cast(tplcodigo as varchar) else tplnomeabreviado end) end) as abreviatura
	from
	    wun.tbtipolograd as tl
) as a
-- public.bth_get_situacao_registro('300', 'tipo-logradouro', left(upper(tl.tplnome),20)) as situacaoRegistro
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-logradouro', descricao))) is null