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
	 	coalesce((
	 		select true
	 		from (
				select distinct
					i2.prdcodigo,
					count(*) as qtd
				from wco.tbitemin i2
				where i2.clicodigo = i.clicodigo
				and i2.minano = i.minano
				and i2.minnro = i.minnro
				and i2.prdcodigo = i.prdcodigo
				group by 1
			) aux
			where aux.qtd > 1
			limit 1)
		, false) as possui_duplicado,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_material_especificacao,
	 	coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-duplicado-especificacao', i.clicodigo, i.minano, i.minnro, '@', i.cmiid))), 0) as id_material_duplicado_epec,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', i.clicodigo, i.minano, i.minnro, '@', 0, '@', i.cmiitem))) as id_gerado
     from wco.tbitemin i
	 where i.clicodigo = {{clicodigo}}
	 and i.minano = {{ano}}
	 and i.minnro in (81)
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
	 	coalesce((
	 		select true
	 		from (
				select distinct
					i2.prdcodigo,
					count(*) as qtd
				from wco.tbitemin i2
				where i2.clicodigo = i.clicodigo
				and i2.minano = i.minano
				and i2.minnro = i.minnro
				and i2.prdcodigo = i.prdcodigo
				group by 1
			) aux
			where aux.qtd > 1
			limit 1)
		, false) as possui_duplicado,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', i.prdcodigo))) as id_material,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', i.prdcodigo))) as id_material_especificacao,
	 	coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-duplicado-especificacao', i.clicodigo, i.minano, i.minnro, '@', i.cmiid))), 0) as id_material_duplicado_epec,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', i.clicodigo, i.minano, i.minnro, '@', i.lotcodigo, '@', i.cmiitem))) as id_gerado
     from wco.tbitemin i
	 where i.clicodigo = {{clicodigo}}
	 and i.minano = {{ano}}
	 and i.minnro in (81)
	 and i.lotcodigo is not null
	 order by 2, 3 desc, 4 desc, 5 asc, 6 asc)
) tab
where id_gerado is null
and id_processo is not null
and id_material is not null
and id_material_especificacao is not null
--and id_processo in (212638, 212707, 212749, 213022, 213025, 212558, 213019, 212741, 212734, 212837, 212869, 212449, 212305, 212865, 212931, 212302, 212913, 212951, 213004, 212270, 212570, 212703, 212288, 212318, 212345, 212568, 212613, 212712, 212866, 212935, 212987, 213031, 212955, 212958, 212250, 212399, 212422, 212451, 212499, 212426, 212519, 212529, 212304, 212389, 212395, 212930, 212667, 212651, 212710, 212647, 213301, 213295, 213285, 213276, 213383, 213377, 213132, 213139, 213201, 213169, 213190, 213387, 213388, 213390, 213200, 213240, 213248, 213256, 213141, 213138, 213328, 213334, 213347, 213363, 213318, 213646, 213624, 213602, 213596, 213399, 213394, 213393, 213533, 213525, 213416, 213419, 213515, 213499, 213494, 213414, 213481, 213456, 213427, 213421, 213413, 213401, 213409, 213411, 213609, 213538, 213408, 213428, 213497, 213569, 213636, 213763, 213441, 213449, 213631, 213705, 213667, 213698, 213700, 213709, 213731, 213735, 213758, 213768)
order by clicodigo, ano_processo desc, nro_processo desc, numero_item asc