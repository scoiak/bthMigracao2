select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-sf-item' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_contrato, '/', ano_contrato, ' (', identificador_contrato, ')') as contratacao,
	concat(nro_sf, '/', ano_sf) as sf,
	'@' as separador,
	*
from (
	select
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		ct.ctrano as ano_contrato,
		ct.ctrnro as nro_contrato,
		ct.ctridentificador as identificador_contrato,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.cmiid,
		c.itcqtde as quantidade,
		c.itcvlrunit as valor_unitario,
		c.itcvlrtotal as valor_total,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, ct.ctrano, ct.ctrnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_solicitacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-item', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.cmiid))) as id_contratacao_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', c.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', c.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-item', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro, '@', c.cmiid))) as id_gerado
	from wco.tbitemcompra c
	inner join wco.tbcompra cp on (cp.clicodigo = c.clicodigo and cp.minano = c.minano and cp.minnro = c.minnro and cp.copano = c.copano and cp.copnro = c.copnro)
	inner join wco.tbcontrato ct on (ct.clicodigo = c.clicodigo and ct.minano = c.minano and ct.minnro = c.minnro and ct.unicodigo = cp.unicodigo)
	where c.clicodigo = {{clicodigo}}
	and c.minano = {{ano}}
	and c.minnro = 62
	and c.minano is not null
	and c.minnro is not null
) tab
where id_gerado is null
and id_solicitacao is not null
and id_contratacao_item is not null
and id_material is not null
and id_especificacao is not null
limit 1