select * from (
	select
		estado.estcodigo as id,
		left(estado.estnome, 20) as chave_1,
		left(estado.estnome, 20) as nome,
		estado.estsigla as uf,
		pais.painome as paisnome,
		COALESCE(public.bth_get_id_gerado('300', 'paises', pais.painome), 29) as pais,
		public.bth_get_situacao_registro('300', 'estados', estado.estnome) as situacao_registro
	from wun.tbestado estado
	inner join wun.tbpais pais ON pais.paisiglaiso = estado.paisiglaiso
) as tab
where situacao_registro in (0)