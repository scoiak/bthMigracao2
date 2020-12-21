select
	row_number() over() as id,
	'305' as sistema,
	'tipo-documento' as tipo_registro,
	*
from (
	select distinct
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))) as chave_dsk1,
       doccodigo as chave_dsk2,
       left(docdescricao,100) as nome_docto,
       coalesce(docqtdediasvenc,0) as dias_validade,
       false as certidao,
       'OUTROS' as tipo_certidao,
       null as observacao,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,
	   																							  'tipo-documento',
	   																							   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))),
	   																							   doccodigo))) as id_gerado
	from wco.tbcaddoc
	where wco.tbcaddoc.docdescricao is not null
	order by 1, 2
) as tab
where id_gerado is null
--limit 2