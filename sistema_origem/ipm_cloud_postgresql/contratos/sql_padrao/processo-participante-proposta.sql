select
	row_number() over() as id,
	'305' as sistema,
	'processo-participante-proposta' as tipo_registro,
	*
from (
	select
		qcp.clicodigo,
		qcp.minano as ano_processo,
		qcp.minnro as nro_processo,
		qcp.cmiid,
		qcp.unicodigo,
		(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_participante,
		qcp.qcpvlrunit as valor_unitario,
		i.cmiqtde as quantidade,
		left(mc.mardescricao, 20) as marca,
		(case when qcp.qcpvencedor = 1 then 'VENCEU' else 'PERDEU' end) as situacao,
		(case
			when qcp.qcpvencedor = 1 then 1
			when qcp.qcpvencedor <> 1 and qcp.qcpposicao = 0 then 2
			else qcp.qcpposicao
		end) as colocacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', qcp.clicodigo, qcp.minano, qcp.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', qcp.clicodigo, qcp.minano, qcp.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', qcp.clicodigo, qcp.minano, qcp.minnro, '@', coalesce(i.lotcodigo, 0) , '@', i.cmiitem))) as id_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante-proposta', qcp.clicodigo, qcp.minano, qcp.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), qcp.cmiid))) as id_gerado
	from wco.tbcadqcp qcp
	left join wco.tbitemin i on (i.clicodigo = qcp.clicodigo and i.minano = qcp.minano and i.minnro = qcp.minnro and i.cmiid = qcp.cmiid)
	left join wun.tbmarca mc on (mc.marcodigo = qcp.marcodigo)
	inner join wun.tbunico u on (u.unicodigo = qcp.unicodigo)
	order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_processo is not null
and id_participante is not null
and id_item is not null