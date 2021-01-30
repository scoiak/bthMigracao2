select
	row_number() over() as id,
	'305' as sistema,
	'processo-despesa' as tipo_registro,
	*
from (
	select distinct
		d.clicodigo,
		d.minano as ano_processo,
		d.minnro as nro_processo,
		d.clicodigopln,
		d.dotcodigo,
		d.loaano,
		0.01 as valor_estimado,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', d.clicodigo, d.minano, d.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'despesa', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', d.clicodigopln))), d.loaano, d.dotcodigo))) as id_despesa,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', d.minano))) as id_exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-despesa', d.clicodigo, d.minano, d.minnro, d.dotcodigo))) as id_gerado
	from wco.tbdotmin d
	where d.clicodigo = {{clicodigo}}
	and d.minano = {{ano}}
	and d.dotcodigo is not null
	order by 1, 2 desc, 3 desc, 4
) tab
where id_gerado is null
and id_exercicio is not null
and id_processo is not null
and id_despesa is not null
--limit 1