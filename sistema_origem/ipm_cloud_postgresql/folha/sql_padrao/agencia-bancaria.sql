select * from (
select
	 row_number() OVER () as id,
	 -- public.bth_get_id_gerado('300', 'banco', cast(b.bcocodigo as varchar)) as banco,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','banco', cast(ba.bcocodigo as varchar)))) as banco,
	 bcanome as nome,
	 bcaagencia as numero,
	 bcadigito as digito
	-- public.bth_get_id_gerado('300', 'cidades', cast(c.cidnome as text)) as municipio
	-- (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','cidades', (select c.cidnome from wun.tbcidade as c where c.cidcodigo = ba.cidcodigo limit 1)))) as municipio,
	from wun.tbbancoagencia as ba
) as a
-- where public.bth_get_situacao_registro('300', 'agencia-bancaria', cast(numero as varchar), cast(banco as varchar)) in (0)
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'agencia-bancaria',cast(numero as varchar), cast(banco as varchar)))) is null
and banco is not null