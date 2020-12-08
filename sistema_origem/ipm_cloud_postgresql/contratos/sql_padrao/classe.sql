select
	row_number() over() as id,
	'305' as sistema,
	'classe' as tipo_registro,
	*
from (
	select
    	cast(grpcodigo as text) as chave_dsk1,
    	cast(clacodigo as text) as chave_dsk2,
    	left(cladescricao, 100) as nome_classe,
    	left(cladescricao, 1000) as espec_classe,
    	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'grupo', grpcodigo))) as id_grupo,
    	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'classe', grpcodigo, clacodigo))) as id_gerado
  from wun.tbclasse
) tab
where id_gerado is null
and id_grupo is not null
order by chave_dsk1, chave_dsk2::integer