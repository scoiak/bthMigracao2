--update public.controle_migracao_registro set id_gerado = 13833 where tipo_registro = 'local-entrega' and i_chave_dsk2 = '406' and id_gerado is null;
--update public.controle_migracao_registro set id_gerado = 13840 where tipo_registro = 'local-entrega' and i_chave_dsk2 = '418' and id_gerado is null;
--update public.controle_migracao_registro set id_gerado = 13815 where tipo_registro = 'local-entrega' and i_chave_dsk2 = '488' and id_gerado is null;

select
	row_number() over() as id,
	'305' as sistema,
	'local-entrega' as tipo_registro,
	*
from (
	select distinct
	       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))) as chave_dsk1,
	       wco.tblocalentrega.loccodigo as chave_dsk2,
	       left(locdescricao, 100) as descricao,
	       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,
	       																							  'local-entrega',
	       																							  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))),
	       																							  wco.tblocalentrega.loccodigo))) as id_gerado
	 from wco.tblocalentminuta, wco.tblocalentrega
	 where wco.tblocalentminuta.loccodigo = wco.tblocalentrega.loccodigo
	 and clicodigo = {{clicodigo}}
	 order by 1, 2
) as tab
where id_gerado is null
--limit 5