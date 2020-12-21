/*
update public.controle_migracao_registro
set id_gerado = 9
where tipo_registro = 'tipo-publicacao'
and hash_chave_dsk = '75288f4d61697273e5b7af8dae15e0e6'
and id_gerado is null
*/

select
	row_number() over() as id,
	'305' as sistema,
	'tipo-publicacao' as tipo_registro,
	*
from (
	select distinct
	       pbltipo as chave_dsk1,
	       (case pbltipo when 1 then 'Edital' else 'Outros' end) as descricao,
	       (case pbltipo when 1 then 'EDITAL' else 'OUTROS' end) as classificacao,
	       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'tipo-publicacao', pbltipo))) as id_gerado
	  FROM wco.tbpublico
	  where pbltipo is not null
	 order by 2
) as tab
where id_gerado is null