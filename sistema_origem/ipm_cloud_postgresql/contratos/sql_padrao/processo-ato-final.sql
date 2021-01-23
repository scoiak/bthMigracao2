select
	row_number() over() as id,
	'305' as sistema,
	'processo-ato-final' as tipo_registro,
	*
from (
	select
		l.clicodigo,
		l.minano as ano_processo,
		l.minnro as nro_processo,
		null as observacoes,
		null as id_tipo_revogacao_anulacao,
		'HOMOLOGACAO_E_ADJUDICACAO' as tipo,
		concat(l.licdatahomologacao::varchar, ' 00:00:00') as data_ato_afinal,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', l.clicodigo, l.minano, l.minnro))) as id_processo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'responsavel', (select (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) from wun.tbunico u where u.unicodigo = (select i.unicodigo from wco.tbintegrante i where i.cmlcodigo = m.cmlcodigo and i.mbcatribuicao in(3,6) limit 1))))), 0) as id_responsavel,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-ato-final', l.clicodigo, l.minano, l.minnro))) as id_gerado
	from wco.tblicitacao l
	inner join wco.tbminuta m on (m.clicodigo = l.clicodigo and m.minano = l.clicodigo and m.minnro = l.minnro)
	where l.licdatahomologacao is not null
	and l.clicodigo = {{clicodigo}}
	and l.minano = {{ano}}
	--and l.minnro = 4
	and l.minnro not in (35, 41, 65, 81, 88, 90, 92, 99, 100)
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
--limit 1