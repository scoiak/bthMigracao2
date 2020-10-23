select * from (
		 c.cidcodigo as id,
		 -- public.bth_get_id_gerado('300', 'estado', e.estnome) as estado,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', e.estnome))) as estado
         c.cidnome as nome,
		 cast(c.cidcep as varchar) as cep
	from wun.tbcidade as c
	inner join wun.tbestado as e on e.estcodigo = c.estcodigo
) as tab
where public.bth_get_situacao_registro('300', 'municipio', nome) in (0)