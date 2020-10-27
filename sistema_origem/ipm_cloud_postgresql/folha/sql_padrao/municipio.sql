select * from (
select
	 c.cidcodigo as id,
	 -- public.bth_get_id_gerado('300', 'estado', e.estnome) as estado,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = c.estcodigo limit 1)))) as estado,
     left(c.cidnome,50) as nome,
	 cast(c.cidcep as varchar) as cep
	from wun.tbcidade as c
) as tab
-- where public.bth_get_situacao_registro('300', 'municipio', nome, cast(estado as varchar)) in (0)
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'municipio', nome, cast(estado as varchar)))) is null
and estado is not null