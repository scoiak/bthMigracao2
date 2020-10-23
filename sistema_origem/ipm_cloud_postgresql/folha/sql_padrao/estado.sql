select * from (
	select
		e.estcodigo as id,
		left(e.estnome, 20) as nome,
		e.estsigla as uf,
		-- public.bth_get_id_gerado('300', 'pais', pais.painome) as pais,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','pais', p.nome))) as pais
	from wun.tbestado as e
	inner join wun.tbpais as p ON p.paisiglaiso = e.paisiglaiso
) as tab
where public.bth_get_situacao_registro('300', 'estado', estnome) in (0)