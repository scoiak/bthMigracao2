select
	row_number() over() as id,
	'305' as sistema,
	'compra-direta-despesa' as tipo_registro,
	*
from (
	select
		c.clicodigo,
		c.copano as ano_cd,
		c.copnro as nro_cd,
		c.dotcodigo,
		trunc((select sum(icd.itcvlrtotal) from  wco.tbitemcompra icd where icd.clicodigo = c.clicodigo and icd.copano = c.copano and icd.copnro = c.copnro), 2) as valor_estimado,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', c.copano))) as id_exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'despesa', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', c.clicodigo))), c.copano, c.dotcodigo))) as id_despesa,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', c.clicodigo, c.copano, c.copnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-despesa', c.clicodigo, c.copano, c.copnro, c.dotcodigo))) as id_gerado
	from wco.tbcompra c
	where c.clicodigo = {{clicodigo}}
	and c.minano is null
	and c.minnro is null
	and c.copano = {{ano}}
	order by 1, 2 desc, 3 asc
) tab
where id_gerado is null
and id_contratacao is not null
and id_despesa is not null
and id_exercicio is not null
--limit 1