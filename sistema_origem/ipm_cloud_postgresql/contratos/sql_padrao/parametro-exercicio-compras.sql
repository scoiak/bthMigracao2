select
	row_number() over() as id,
	'305' as sistema,
	'parametro-exercicio-compra' as tipo_registro,
	*
from (
	SELECT distinct organo,
	       1 as chave_dsk1,
	       organo as chave_dsk2,
	      'EXERCICIO' as numero_licit,
	      'Sequencial único por exercício' as descricao,
	      (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', organo))) as id_parametro_exercicio,
	      (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'parametro-exercicio-compras', organo))) as id_gerado
	  from wun.tborgao
) tab
 where id_gerado is null
 and id_parametro_exercicio is not null
 order by organo