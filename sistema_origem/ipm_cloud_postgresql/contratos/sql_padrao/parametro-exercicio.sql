select
	row_number() over() as id,
	'305' as sistema,
	'parametro-exercicio' as tipo_registro,
	*
from (
	select distinct
        organo as chave_dsk1,
        (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', organo))) as id_configuracao_organograma,
		(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', organo))) as id_gerado
	from wun.tborgao
	where organo >= 2005
) tab
where id_gerado is null
and id_configuracao_organograma is not null