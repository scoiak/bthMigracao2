-- update wun.tbpais set painome = 'Aústria' where painome = 'Áustria'
-- update wun.tbestado set estnome = 'Distrito Federal' where estnome = 'Federal'

select * from (
select
		e.estcodigo as id,
		left(e.estnome, 20) as nome,
		e.estsigla as uf,
		-- public.bth_get_id_gerado('300', 'pais', pais.painome) as pais,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','pais',(select p.painome from wun.tbpais as p where p.paisiglaiso = e.paisiglaiso)))) as pais
	from wun.tbestado as e
) as tab
-- where public.bth_get_situacao_registro('300', 'estado', nome) in (0)
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'estado', nome))) is null
and pais is not null