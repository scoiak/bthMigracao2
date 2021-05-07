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
	left join wco.tbminuta m on (m.clicodigo = l.clicodigo and m.minano = l.clicodigo and m.minnro = l.minnro)
	where l.licdatahomologacao is not null
	and l.licsituacao = 4
	and l.clicodigo = {{clicodigo}}
	and l.minano = {{ano}}
	--and l.minnro = 120
	and l.minnro in (20, 15, 10)
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
--and id_processo in (212638, 212707, 212749, 213022, 213025, 212558, 213019, 212741, 212734, 212837, 212869, 212449, 212305, 212865, 212931, 212302, 212913, 212951, 213004, 212270, 212570, 212703, 212288, 212318, 212345, 212568, 212613, 212712, 212866, 212935, 212987, 213031, 212955, 212958, 212250, 212399, 212422, 212451, 212499, 212426, 212519, 212529, 212304, 212389, 212395, 212930, 212667, 212651, 212710, 212647, 213301, 213295, 213285, 213276, 213383, 213377, 213132, 213139, 213201, 213169, 213190, 213387, 213388, 213390, 213200, 213240, 213248, 213256, 213141, 213138, 213328, 213334, 213347, 213363, 213318, 213646, 213624, 213602, 213596, 213399, 213394, 213393, 213533, 213525, 213416, 213419, 213515, 213499, 213494, 213414, 213481, 213456, 213427, 213421, 213413, 213401, 213409, 213411, 213609, 213538, 213408, 213428, 213497, 213569, 213636, 213763, 213441, 213449, 213631, 213705, 213667, 213698, 213700, 213709, 213731, 213735, 213758, 213768)
--limit 1