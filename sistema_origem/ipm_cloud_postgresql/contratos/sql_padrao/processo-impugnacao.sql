select
	row_number() over() as id,
	'305' as sistema,
	'processo-impugnacao' as tipo_registro,
	'@' as separador,
	*
from (
	select
		i.clicodigo,
		i.minano as ano_processo,
		i.minnro as nro_processo,
		i.intsequencia as sequencial,
		i.unicodigo,
		(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_cnpj,
		i.intdataentrada::varchar as data_impugnacao,
		i.intdataentrada::varchar as data_publicacao,
		left(coalesce(i.intparinterpositor, 'Não informado'), 1000) as motivo,
		'NAO_JULGADA' as situacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro, '@', i.intsequencia))) as id_gerado
	from wco.tbinterp i
	inner join wun.tbunico u on (u.unicodigo = i.unicodigo)
	where i.clicodigo = 2016--{{clicodigo}}
	order by 1, 2 desc, 3 desc, 4
) tab
where id_gerado is null
and id_processo is not null
and id_fornecedor is not null
limit 5