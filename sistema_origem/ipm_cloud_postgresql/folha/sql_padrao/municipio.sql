select * from (
		 select '300' as sistema,
		 cidade.cidcodigo as id,
		 COALESCE(public.bth_get_id_gerado('300', 'estados', estado.estnome), 24) as idEstado,
             cidade.cidnome as nome,
		 CAST(cidade.cidcep as text) as cep,
		 public.bth_get_situacao_registro('300', 'cidades', cidade.cidnome) as situacao_registro
	from wun.tbcidade cidade
	inner join wun.tbestado estado on estado.estcodigo = cidade.estcodigo
) as tab
where situacao_registro in (0)