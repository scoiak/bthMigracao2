select * from (
	select distinct '300' as sistema,
		 'motivo-alteracao-salarial' as tipo_registro,
		 mtrdescricao as id,
		 mtrdescricao as chave_dsk1,
		 public.bth_get_situacao_registro('300', 'motivo-alteracao-salarial', mtrdescricao) as situacao_registro
	from wfp.tbmotivoreajuste
) tab
where situacao_registro in (0)