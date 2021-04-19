select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-arp-item' as tipo_registro,
	concat(minnro, '/', minano) as minuta,
	concat(arpnro, '/', arpano, ' (', arpsequencia, ')') as ata,
	'@' as separador,
	*
from (
	select
		aux.*,
		u.uninomerazao,
		coalesce((
	 		select true
	 		from (
				select distinct
					i2.prdcodigo,
					count(*) as qtd
				from wco.tbitemin i2
				where i2.clicodigo = aux.clicodigo_chave
				and i2.minano = aux.minano
				and i2.minnro = aux.minnro
				and i2.prdcodigo = aux.prdcodigo
				group by 1
			) aux2
			where aux2.qtd > 1
			limit 1)
		, false) as possui_duplicado,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', clicodigo_chave, aux.arpano, aux.arpnro, '@', aux.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', im.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', im.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-duplicado-especificacao', aux.clicodigo_chave, aux.minano, aux.minnro, '@', aux.cmiid))) as id_material_duplicado_epec,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp', clicodigo_chave, aux.arpano, aux.arpnro, aux.unicodigo))) as id_ata,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp-item', clicodigo_chave, aux.arpano, aux.arpnro, aux.unicodigo, aux.cmiid))) as id_item_ata,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante-proposta', clicodigo_chave, aux.minano, aux.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), aux.cmiid))) as id_proposta,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-item', clicodigo_chave, aux.arpano, aux.arpnro, '@', aux.arpsequencia, '@', aux.cmiid))) as id_gerado
	from (
		select
			a.clicodigo as clicodigo_chave,
			i.minano,
			i.minnro,
			a.arpano,
			a.arpnro,
			a.arpsequencia,
			i.cmiid,
			a.unicodigo,
			i.prdcodigo,
			count(*) as qtd_reg_agrupados,
			sum(i.itcqtde - coalesce((select sum(e.iesqtde) from wco.tbitemcompraest e where e.clicodigo = i.clicodigo and e.copano = i.copano and e.copnro = i.copnro and e.itcitem = i.itcitem), 0)) as quantidade,
			sum(i.itcvlrunit) as valor_unitario,
			sum(i.itcvlrtotal - coalesce((select sum(e.iesvlrtotal) from wco.tbitemcompraest e where e.clicodigo = i.clicodigo and e.copano = i.copano and e.copnro = i.copnro and e.itcitem = i.itcitem), 0)) as valor_total
		from wco.tbitemcompra i
		inner join wco.tbcompra c on (c.clicodigo = i.clicodigo and c.minano = i.minano and c.minnro = i.minnro and c.copano = i.copano and c.copnro = i.copnro)
		inner join wco.tbataregpreco a on (a.clicodigo = coalesce(c.clicodigomin, c.clicodigo) and a.minano = i.minano and a.minnro = i.minnro and a.unicodigo = c.unicodigo)
		where true
		and ((c.clicodigo = {{clicodigo}} and c.clicodigomin is null) or ((c.clicodigo = {{clicodigo}} and c.clicodigomin = c.clicodigo)) or (c.clicodigomin = {{clicodigo}} and c.clicodigo <> c.clicodigomin))
		and c.minano = {{ano}}
		--and c.minnro = 27
		--and a.arpnro = 8
		group by 1, 2, 3, 4, 5, 6, 7, 8, 9
		order by 1, 2 desc, 4 desc, 6, 8
	) aux
	inner join wun.tbunico u on (u.unicodigo = aux.unicodigo)
	inner join wco.tbitemin im on (im.clicodigo = clicodigo_chave and im.minano = aux.minano and im.minnro = aux.minnro and im.cmiid = aux.cmiid)
) tab
where true
and id_gerado is null
and id_contratacao is not null
and id_material is not null
and id_especificacao is not null
and id_ata is not null
and id_proposta is not null
and quantidade > 0
--and uninomerazao = 'PLAYRIO PARQUES INFANTIL'
--limit 1