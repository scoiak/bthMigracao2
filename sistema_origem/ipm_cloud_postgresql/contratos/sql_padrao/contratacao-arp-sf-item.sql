select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-arp-sf-item' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_ata, '/', ano_ata) as ata,
	concat(nro_sf, '/', ano_sf) as sf,
	'@' as separador,
	*
from (
	select
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		a.arpano as ano_ata,
		a.arpnro as nro_ata,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.cmiid,
		c.itcqtde as quantidade,
		c.itcvlrunit as valor_unitario,
		c.itcvlrtotal as valor_total,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf', c.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro))) as id_solicitacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-item', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia, '@', c.cmiid))) as id_contratacao_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', c.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', c.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf-item', c.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro, '@', c.cmiid))) as id_gerado
	from wco.tbitemcompra c
	inner join wco.tbcompra cp on (cp.clicodigo = c.clicodigo and cp.minano = c.minano and cp.minnro = c.minnro and cp.copano = c.copano and cp.copnro = c.copnro)
	inner join wco.tbataregpreco a on (a.clicodigo = c.clicodigo and a.minano = c.minano and a.minnro = c.minnro and a.unicodigo = cp.unicodigo)
	where c.clicodigo = {{clicodigo}}
	and c.minano = {{ano}}
	and c.minnro = 93
	and a.arpnro = 69
	and c.minano is not null
	and c.minnro is not null
) tab
where id_gerado is null
and id_solicitacao is not null
and id_contratacao_item is not null
and id_material is not null
and id_especificacao is not null
limit 1