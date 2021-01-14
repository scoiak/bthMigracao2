select
	row_number() over() as id,
	'305' as sistema,
	'processo-interposicao' as tipo_registro,
	*
from (
	select
		i.clicodigo,
		i.minano as ano_processo,
		i.minnro as nro_processo,
		i.intdataentrada::varchar as data_recurso,
		i.unicodigo,
		(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_participante,
		i.inttiporecurso,
		(case i.inttiporecurso
			when 1 then 63 -- Habilitação
			when 4 then 62 -- Edital
			else null
		end) as id_tipo,
		left(i.intparinterpositor, 2000) as motivo,
		'NAO_ANALISADO' as situacao,
		left(i.intdataentrada::varchar, 4)::integer as ano_protocolo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', i.clicodigo, i.minano, i.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-interposicao', i.clicodigo, i.minano, i.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_gerado
	from wco.tbinterp i
	inner join wun.tbunico u on (u.unicodigo = i.unicodigo)
) tab
where id_gerado is null
and id_processo is not null
and id_participante is not null