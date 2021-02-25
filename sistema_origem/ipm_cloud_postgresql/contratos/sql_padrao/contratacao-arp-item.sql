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
		im.prdcodigo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', coalesce(aux.clicodigomin, aux.clicodigo), aux.arpano, aux.arpnro, '@', aux.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', im.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', im.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp', coalesce(aux.clicodigomin, aux.clicodigo), aux.arpano, aux.arpnro, aux.unicodigo))) as id_ata,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp-item', coalesce(aux.clicodigomin, aux.clicodigo), aux.arpano, aux.arpnro, aux.unicodigo, aux.cmiid))) as id_item_ata,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante-proposta', coalesce(aux.clicodigomin, aux.clicodigo), aux.minano, aux.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), aux.cmiid))) as id_proposta,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-item', coalesce(aux.clicodigomin, aux.clicodigo), aux.arpano, aux.arpnro, '@', aux.arpsequencia, '@', aux.cmiid))) as id_gerado
	from (
		select
			i.clicodigo,
			c.clicodigomin,
			i.minano,
			i.minnro,
			a.arpano,
			a.arpnro,
			a.arpsequencia,
			i.cmiid,
			a.unicodigo,
			count(*) as qtd_reg_agrupados,
			sum(i.itcqtde) as quantidade,
			sum(i.itcvlrunit) as valor_unitario,
			sum(i.itcvlrtotal) as valor_total
		from wco.tbitemcompra i
		inner join wco.tbcompra c on (c.clicodigo = i.clicodigo and c.minano = i.minano and c.minnro = i.minnro and c.copano = i.copano and c.copnro = i.copnro)
		inner join wco.tbataregpreco a on (a.clicodigo = c.clicodigomin and a.minano = i.minano and a.minnro = i.minnro and a.unicodigo = c.unicodigo)
		where c.clicodigomin = {{clicodigo}}
		and c.minano = {{ano}}
		and c.minnro = 153
		--and a.arpnro = 69
		group by 1, 2, 3, 4, 5, 6, 7, 8, 9
		order by 1, 2 desc, 4 desc, 6, 8
	) aux
	inner join wun.tbunico u on (u.unicodigo = aux.unicodigo)
	inner join wco.tbitemin im on (im.clicodigo = coalesce(aux.clicodigomin, aux.clicodigo) and im.minano = aux.minano and im.minnro = aux.minnro and im.cmiid = aux.cmiid)
) tab
where id_gerado is null
and id_contratacao is not null
and id_material is not null
and id_especificacao is not null
and id_ata is not null
and id_proposta is not null
--limit 2