select
	row_number() over() as id,
	'305' as sistema,
	'processo-item' as tipo_registro,
	'@' as separador,
	*
from (
	-- Processos 'por item'
	 (select
	 	'por_item' as origem,
	 	i.clicodigo,
	 	i.minano as ano_processo,
	 	i.minnro as nro_processo,
	 	0 as nro_lote,
	 	i.cmiid as numero_item,
	 	i.cmiitem as numero_item_original,
	 	i.cmicotacaomaxima as valor_unitario,
	 	i.cmivalortotal as valor_total,
	 	i.cmiqtde as quantidade,
	 	'N' as somente_mpes,
	 	0 as favorece_me_app,
	 	(case when i.cmiexclusivo = 1 then 100.00 else 0.00 end) as percentual,
	 	(case when i.cmiexclusivo = 1 then 'RESERVADA_MPES' else 'LIVRE' end) as tipo_participacao,
	 	'NAO_SE_APLICA' as tipo_beneficio,
	 	false as amostra,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_material_especificacao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', i.clicodigo, i.minano, i.minnro, '@', 0, '@', i.cmiitem))) as id_gerado	 from wco.tbitemin i
	 where i.clicodigo = {{clicodigo}}
	 and i.lotcodigo is null
	 order by 2, 3 desc, 4 desc, 5 asc, 6 asc)
	 union all
	 -- Processos 'por lote'
	(select
	 	'por_lote' as origem,
	 	i.clicodigo,
	 	i.minano as ano_processo,
	 	i.minnro as nro_processo,
	 	i.lotcodigo as nro_lote,
	 	i.cmiid as numero_item,
	 	i.cmiitem as numero_item_original,
	 	i.cmicotacaomaxima as valor_unitario,
	 	i.cmivalortotal as valor_total,
	 	i.cmiqtde as quantidade,
	 	'N' as somente_mpes,
	 	0 as favorece_me_app,
	 	(case when i.cmiexclusivo = 1 then 100.00 else 0.00 end) as percentual,
	 	(case when i.cmiexclusivo = 1 then 'RESERVADA_MPES' else 'LIVRE' end) as tipo_participacao,
	 	'NAO_SE_APLICA' as tipo_beneficio,
	 	false as amostra,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_material_especificacao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', i.clicodigo, i.minano, i.minnro, '@', i.lotcodigo, '@', i.cmiitem))) as id_gerado
     from wco.tbitemin i
	 where i.clicodigo = {{clicodigo}}
	 and i.lotcodigo is not null
	 order by 2, 3 desc, 4 desc, 5 asc, 6 asc)
) tab
where id_gerado is null
and id_processo is not null
and id_material is not null
and id_material_especificacao is not null
order by clicodigo, ano_processo desc, nro_processo desc, numero_item asc
