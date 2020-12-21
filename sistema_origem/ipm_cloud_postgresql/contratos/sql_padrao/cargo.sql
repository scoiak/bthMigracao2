select
	row_number() over() as id,
	'305' as sistema,
	'cargo' as tipo_registro,
	*
from (
	select distinct
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))) as chave_dsk1,
       unaccent(upper(trim(mbccargo))) as chave_dsk2,
       unaccent(upper(trim(mbccargo))) as nome,
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,
       																							  'cargo',
       																							  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))),
       																							  unaccent(upper(trim(mbccargo)))))) as id_gerado
	 from wco.tbintegrante
	 where mbccargo is not null
	 order by 1, 2
) as tab
where id_gerado is null
--limit 2