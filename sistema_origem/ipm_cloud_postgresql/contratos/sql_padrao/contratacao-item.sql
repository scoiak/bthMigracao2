select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-item' as tipo_registro,
	(case when ano_contrato is not null then 'PROCESSO' else 'ATA_RP' end) as tipo_contrato,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_contrato, '/', ano_contrato, ' (', identificador_contrato, ')') as contrato,
	'@' as separador,
	*
from (
	select distinct
		q.clicodigo,
		q.minano as ano_minuta,
		q.minnro as nro_minuta,
		c.ctrano as ano_contrato,
		c.ctrnro as nro_contrato,
		c.ctridentificador as identificador_contrato,
		q.cmiid,
		i.lotcodigo as lote,
		i.cmiitem as nro_item,
		i.cmiqtde as quantidade,
		q.qcpvlrunit as valor_unitario,
		q.qcpvlrtotal as valor_total,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, c.ctrano, c.ctridentificador))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-ato-final', c.clicodigomin, c.minano, c.minnro))) as id_ato_final,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante-proposta', c.clicodigomin, q.minano, q.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), q.cmiid))) as id_proposta,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-item', c.clicodigo, c.ctrano, c.ctridentificador, '@', q.cmiid))) as id_gerado
	from wco.vw_qcp_vencedor q
	left join wco.tbcontrato c on (c.clicodigo = q.clicodigo and c.minano = q.minano and c.minnro = q.minnro and c.unicodigo = q.unicodigo)
	left join wco.tbitemin i on (i.clicodigo = q.clicodigo and i.minano = q.minano and i.minnro = q.minnro and i.cmiid = q.cmiid)
	left join wun.tbunico u on (u.unicodigo = q.unicodigo)
	where q.clicodigo = {{clicodigo}}
	and q.minano = {{ano}}
	and c.ctrtipoaditivo is null
	and c.ctrano is not null
	--and c.minnro in (204)
	--and c.ctridentificador = 99
	order by 1, 2 desc, 3 desc, q.cmiid
) tab
where id_gerado is null
and id_contratacao is not null
--and id_proposta is not null
and id_material is not null
and id_especificacao is not null
--and id_ato_final is not null
--limit 5