select
	row_number() over() as id,
	'305' as sistema,
	'grupo' as tipo_registro,
	*
from (
 	select
       grpcodigo as chave_dsk1,
       left(grpdescricao, 100) as nome_grupo,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'grupo', grpcodigo))) as id_gerado
	from wun.tbgrupo
) tab
where id_gerado is null
--limit 2