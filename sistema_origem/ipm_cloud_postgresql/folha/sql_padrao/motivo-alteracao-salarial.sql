select  
ROW_NUMBER() OVER() as id,
* from (
	select distinct 					
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as entidade,		 		 
		 mtrdescricao as descricao 
	from wfp.tbmotivoreajuste
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'motivo-alteracao-salarial', entidade, descricao))) is null