select
	row_number() over() as id,
	'305' as sistema,
	'processo-documento' as tipo_registro,
	*
from (
	select
		d.clicodigo,
		d.minano as ano_processo,
		d.minnro as nro_processo,
		d.doccodigo as nro_documento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', d.clicodigo, d.minano, d.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'tipo-documento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', d.clicodigo))), d.doccodigo))) as id_tipo_documento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-documento', d.clicodigo, d.minano, d.minnro, d.doccodigo))) as id_gerado
	from wco.tbdocexigido d
	where clicodigo = 2016 --{{clicodigo}}
	order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_tipo_documento is not null
and id_processo is not null
limit 10