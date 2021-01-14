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
	 where i.clicodigo = 2016
	 and i.lotcodigo is not null
	 order by 1, 2 desc, 3 desc, 4 asc, 5 asc
) tab
where id_gerado is null
and id_processo is not null
and id_lote is not null
and id_item is not null
--limit 7