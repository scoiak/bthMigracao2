select *
from (
	select distinct
		'300' as sistema,
      	'motivo-alteracao-cargo' as tipo_registro,
       	alccodigo as id,
	   	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as chave_dsk1,
	   	alcdescricao as chave_dsk2,
	   	clicodigo,
	   	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as id_entidade,
       	alcdescricao as descricao,
       	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300' , 'motivo-alteracao-cargo', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))), alcdescricao))) as id_gerado
	from wfp.tbaltcargo
	order by alccodigo
) tab
where coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300' , 'motivo-alteracao-cargo', chave_dsk1, chave_dsk2))), 0) in (0)