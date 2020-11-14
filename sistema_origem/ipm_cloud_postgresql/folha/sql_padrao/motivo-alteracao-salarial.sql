select * from (
	select distinct '300' as sistema,
		 'motivo-alteracao-salarial' as tipo_registro,
		 1 as id,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as chave_dsk1,
		 --'2734' as chave_dsk1,
		 mtrdescricao as chave_dsk2,
		 mtrdescricao as descricao,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as id_entidade,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'motivo-alteracao-salarial', clicodigo, mtrdescricao))) as situacao_registro
	from wfp.tbmotivoreajuste
) tab
where situacao_registro in (0)