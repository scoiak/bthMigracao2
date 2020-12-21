select
	row_number() over() as id,
	'305' as sistema,
	'tipo-interposicao-recurso' as tipo_registro,
	*
from (
	select
	   inttiporecurso as chave_dsk1,
	   (case inttiporecurso when 1 then 'Habilitação' when 4 then 'Edital' else 'Outros' end) as descricao,
	   (case inttiporecurso when 1 then 'HABILITACAO' else 'OUTROS' end) as classificacao,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'tipo-interposicao-recurso', inttiporecurso))) as id_gerado
	from wco.tbinterp
) as tab
where id_gerado is null