select
	row_number() over() as id,
	'305' as sistema,
	'processo-entidade-item' as tipo_registro,
	'@' as separador,
	*
from (
	select
		r.clicodigomin,
		r.minano as ano_processo,
		r.minnro as nro_processo,
		r.clicodigoreq,
		r.cmiid,
		i.lotcodigo,
		i.cmiitem,
		ritqtdeutilizada as qtd_distribuida,
		coalesce(i.lotcodigo, 0) as nro_lote,
		i.cmiitem as numero_item_original,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', r.clicodigomin, r.minano, r.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', r.clicodigomin, r.minano, r.minnro, '@', coalesce(i.lotcodigo, 0), '@', i.cmiitem))) as id_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', r.clicodigoreq))) as id_entidade_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-entidade-item', r.clicodigomin, r.minano, r.minnro, '@', coalesce(i.lotcodigo, 0), '@', i.cmiitem, '@', r.clicodigoreq)))  as id_gerado
	from wco.tbreqitemin r
	natural join wco.tbitemin i
	where r.clicodigomin = {{clicodigo}}
	order by 1, 2 desc, 3 desc, 5 asc
) tab
where id_gerado is null
and id_processo is not null
and id_item is not null
and id_entidade_participante is not null
limit 1