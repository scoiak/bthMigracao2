select
	row_number() over() as id,
	'305' as sistema,
	'processo-convidado' as tipo_registro,
	*
from (
	select
		p.clicodigo,
		p.minano ano_processo,
		p.minnro nro_processo,
		p.unicodigo,
		(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_cnpj,
		u.uninomerazao as nome_fornecedor,
		concat((select m.mindata from wco.tbminuta m where m.clicodigo = p.clicodigo and m.minano = p.minano and m.minnro = p.minnro)::varchar, ' 00:00:00') as data_convite,
		null as data_recebimento,
		'N' as auto_convocacao,
		null as nro_protocolo,
		null as observacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', p.clicodigo, p.minano, p.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-convidado', p.clicodigo, p.minano, p.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_gerado
	from wco.tbparlic p
	inner join wun.tbunico u on (u.unicodigo = p.unicodigo)
	where p.clicodigo = {{clicodigo}}
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_fornecedor is not null
limit 10