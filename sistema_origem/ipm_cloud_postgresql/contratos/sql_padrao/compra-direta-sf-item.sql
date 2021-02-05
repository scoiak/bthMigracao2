select
	row_number() over() as id,
	'305' as sistema,
	'compra-direta-sf-item' as tipo_registro,
	concat(nro_sf, '/', ano_sf) as contratacao,
	concat(nro_sf, '/', ano_sf) as sf,
	'@' as separador,
	*
from (
	select
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.itcitem,
		c.itcqtde as quantidade,
		c.itcvlrunit as valor_unitario,
		c.itcvlrtotal as valor_total,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', c.clicodigo, c.copano, c.copnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-sf', c.clicodigo, c.copano, c.copnro))) as id_solicitacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-item', c.clicodigo, c.copano, c.copnro, c.itcitem))) as id_contratacao_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', c.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', c.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-sf-item', c.clicodigo, c.copano, c.copnro, '@', c.itcitem))) as id_gerado
	from wco.tbitemcompra c
	where c.clicodigo = {{clicodigo}}
	and c.copano = {{ano}}
	--and c.copnro = 1402
	and c.minano is null
	and c.minnro is null
) tab
where id_gerado is null
and id_solicitacao is not null
and id_contratacao_item is not null
and id_material is not null
and id_especificacao is not null
--limit 1