select 
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300' , 'motivo-alteracao-cargo', entidade, descricao))) as idCloud,
*
from (
	select distinct
       	alccodigo as id,
	   	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as entidade,
	   	alcdescricao as descricao
	from wfp.tbaltcargo
	order by alccodigo
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300' , 'motivo-alteracao-cargo', entidade, descricao))) is null