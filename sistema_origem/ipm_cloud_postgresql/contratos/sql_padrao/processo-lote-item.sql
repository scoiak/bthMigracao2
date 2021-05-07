select
	row_number() over() as id,
	'305' as sistema,
	'processo-lote-item' as tipo_registro,
	'@' as separador,
	*
from (
	select
	 	i.clicodigo,
	 	i.minano as ano_processo,
	 	i.minnro as nro_processo,
	 	i.lotcodigo as nro_lote,
	 	i.cmiitem as numero_item_original,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-lote', i.clicodigo, i.minano, i.minnro, i.lotcodigo))) as id_lote,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', i.clicodigo, i.minano, i.minnro, '@', i.lotcodigo, '@', i.cmiitem))) as id_item,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-lote-item', i.clicodigo, i.minano, i.minnro, '@', i.lotcodigo, '@', i.cmiitem))) as id_gerado
	 from wco.tbitemin i
	 where i.clicodigo = {{clicodigo}}
	 and i.minano = {{ano}}
	 and i.minnro in (20, 15, 10)
	 and i.lotcodigo is not null
	 order by 1, 2 desc, 3 desc, 4 asc, 5 asc
) tab
where id_gerado is null
and id_processo is not null
and id_lote is not null
and id_item is not null
--and id_processo in (212638,212707,212749,213022,213025,212558,213019,212741,212734,212837,212869,212449,212305,212865,212931,212302,212913,212951,213004,212270,212570,212703,212288,212318,212345,212568,212613,212712,212866,212935,212987,213031,212955,212958,212250,212399,212422,212451,212499,212426,212519,212529,212304,212389,212395,212930,212667,212651,212710,212647,213285,213132,213190,213256,213399,213394,213393,213414,213421,213413,213401,213409,213411,213408,213428,213497,213763,213449,213758)
--limit 7