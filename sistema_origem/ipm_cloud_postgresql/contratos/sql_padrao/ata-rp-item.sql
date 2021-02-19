select
	row_number() over() as id,
	'305' as sistema,
	'ata-rp-item' as tipo_registro,
	*
from (
	select
	 	rp.clicodigo,
	 	rp.arpano as ano_ata,
	 	rp.arpnro as nro_ata,
	 	rp.minano as ano_processo,
	 	rp.minnro as nro_processo,
	 	rp.unicodigo,
	 	(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_fornecedor,
	    'ATIVO' AS situacao,
	    qcp.cmiid,
	    coalesce(i.lotcodigo, 0) as lote,
	    i.cmiitem,
	    i.prdcodigo,
	    i.cmiqtde as quantidade,
	    qcp.qcpvlrunit as valor_unitario,
	    qcp.qcpvlrtotal as valor_total,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', rp.clicodigo, rp.minano, rp.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp', rp.clicodigo, rp.arpano, rp.arpnro, rp.unicodigo))) as id_ata,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_material_especificacao,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', rp.clicodigo, rp.minano, rp.minnro, '@', coalesce(i.lotcodigo ,0), '@', i.cmiitem))) as id_item,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item-configuracao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', rp.clicodigo, rp.minano, rp.minnro))), (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', rp.clicodigo, rp.minano, rp.minnro, '@', coalesce(i.lotcodigo ,0), '@', i.cmiitem)))))) as id_config_item,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp-item', rp.clicodigo, rp.arpano, rp.arpnro, rp.unicodigo, qcp.cmiid))) as id_gerado
	 from wco.tbataregpreco rp
	 inner join wun.tbunico u on u.unicodigo = rp.unicodigo
	 left join wco.vw_qcp_vencedor qcp on (qcp.clicodigo = rp.clicodigo
	 								and qcp.minano = rp.minano
	 								and qcp.minnro = rp.minnro
	 								and qcp.unicodigo = rp.unicodigo)
	 left join wco.tbitemin i on (i.clicodigo = rp.clicodigo and i.minano = rp.minano and i.minnro = rp.minnro and i.cmiid = qcp.cmiid)
	 where rp.clicodigo = {{clicodigo}}
	 and rp.minano = {{ano}}
	 --and rp.minnro = 35
	 order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_ata is not null
and id_processo is not null
and id_material is not null
and id_material_especificacao is not null
and id_item is not null
and id_config_item is not null
--limit 1