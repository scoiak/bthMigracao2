select
	row_number() over() as id,
	'305' as sistema,
	'compra-direta-item' as tipo_registro,
	*
from (
	select
		ic.clicodigo,
		ic.copano as ano_cd,
		ic.copnro as nro_cd,
		ic.itcitem,
		ic.prdcodigo,
		ic.itcqtde as quantidade,
		ic.itcvlrunit as valor_unitario,
		ic.itcitem as numero,
		null as marca,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', ic.clicodigo, ic.copano, ic.copnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', ic.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', ic.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-item', ic.clicodigo, ic.copano, ic.copnro, ic.itcitem))) as id_gerado
	from wco.tbitemcompra ic
	where ic.clicodigo = {{clicodigo}}
	and ic.copano = {{ano}}
	--and ic.copnro = 1
	and ic.minano is null
	and ic.minnro is null
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_contratacao is not null
and id_material is not null
and id_especificacao is not null
--limit 10