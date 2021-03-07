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
		a.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		a.arpano as ano_ata,
		a.arpnro as nro_ata,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.cmiid,
		(c.itcqtde - coalesce((select sum(e.iesqtde) from wco.tbitemcompraest e where e.clicodigo = c.clicodigo and e.copano = c.copano and e.copnro = c.copnro and e.itcitem = c.itcitem), 0)) as quantidade,
		c.itcvlrunit as valor_unitario,
		(c.itcvlrtotal - coalesce((select sum(e.iesvlrtotal) from wco.tbitemcompraest e where e.clicodigo = c.clicodigo and e.copano = c.copano and e.copnro = c.copnro and e.itcitem = c.itcitem), 0)) as valor_total,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf', a.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro))) as id_solicitacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-item', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia, '@', c.cmiid))) as id_contratacao_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', c.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', c.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf-item', a.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro, '@', c.cmiid))) as id_gerado
	from wco.tbitemcompra c
	left join wco.tbcompra cp on (cp.clicodigo = c.clicodigo and cp.minano = c.minano and cp.minnro = c.minnro and cp.copano = c.copano and cp.copnro = c.copnro)
	left join wco.tbataregpreco a on (a.clicodigo = coalesce(cp.clicodigomin, cp.clicodigo) and a.minano = c.minano and a.minnro = c.minnro and a.unicodigo = cp.unicodigo)
	where true
	and ((c.clicodigo = {{clicodigo}} and c.clicodigomin is null) or ((c.clicodigo = {{clicodigo}} and c.clicodigomin = c.clicodigo)) or (c.clicodigomin = {{clicodigo}} and c.clicodigo <> c.clicodigomin))
	and cp.minano = {{ano}}
	--and cp.minnro = 148
	--and a.arpnro = 13
	and c.minano is not null
	and c.minnro is not null
) tab
where id_gerado is null
and id_solicitacao is not null
and id_contratacao_item is not null
and id_material is not null
and id_especificacao is not null
and quantidade > 0
--limit 1