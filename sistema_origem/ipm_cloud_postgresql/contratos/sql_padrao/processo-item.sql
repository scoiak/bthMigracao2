select
	row_number() over() as id,
	'305' as sistema,
	'processo-item' as tipo_registro,
	*
from (
	-- Itens cota livre
	 (select
	 	'cota_livre' as origem,
	 	i.clicodigo,
	 	i.minano as ano_processo,
	 	i.minnro as nro_processo,
	 	i.cmiitem as numero_item,
	 	i.cmicotacaomaxima as valor_unitario,
	 	i.cmivalortotal as valor_total,
	 	i.cmiqtde as quantidade,
	 	'N' as somente_mpes,
	 	0 as favorece_me_app,
	 	0.00 as percentual,
	 	'LIVRE' as tipo_participacao,
	 	'NAO_SE_APLICA' as tipo_beneficio,
	 	false as amostra,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_material_especificacao,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', i.clicodigo, i.minano, i.minnro, i.cmiitem))) as id_gerado
	 from wco.tbitemin i
	 where i.clicodigo = 2016
	 and i.lotcodigo is null
	 and (i.cmiexclusivo is null or i.cmiexclusivo <> 1)
	 order by 1, 2 desc, 3 desc, 4 asc)
	 union all
	 -- Itens cota reservada
	 (select
	 	'cota_reservada' as origem,
	 	i.clicodigo,
	 	i.minano as ano_processo,
	 	i.minnro as nro_processo,
	 	i.cmiitem as numero_item,
	 	i.cmicotacaomaxima as valor_unitario,
	 	i.cmivalortotal as valor_total,
	 	i.cmiqtde as quantidade,
	 	'N' as somente_mpes,
	 	0 as favorece_me_app,
	 	100 as percentual,
	 	'RESERVADA_MPES' as tipo_participacao,
	 	'NAO_SE_APLICA' as tipo_beneficio,
	 	false as amostra,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_material_especificacao,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', i.clicodigo, i.minano, i.minnro, i.cmiitem))) as id_gerado
	 from wco.tbitemin i
	 where i.clicodigo = 2016
	 and i.lotcodigo is null
	 and i.cmiexclusivo = 1
	 order by 1, 2 desc, 3 desc, 4 asc)
) tab
where id_gerado is null
and id_processo is not null
and id_material is not null
and id_material_especificacao is not null