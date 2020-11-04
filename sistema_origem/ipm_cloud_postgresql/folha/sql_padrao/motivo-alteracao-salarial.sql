select * from (
	select 
		 row_number() over() as id,
		 mtrdescricao as descricao
	from wfp.tbmotivoreajuste where odomesano = 202009
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'motivo-alteracao-salarial',descricao))) is null