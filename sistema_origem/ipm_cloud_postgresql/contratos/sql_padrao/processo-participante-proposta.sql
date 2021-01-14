select
	row_number() over() as id,
	'305' as sistema,
	'processo-participante-proposta' as tipo_registro,
	*
from (
	select distinct
		aux.*,
		(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_participante,
		0.00 as valor_unitario,
		i.cmiqtde as quantidade,
		null as marca,
		'NAO_COTOU' as situacao,
		null as colocacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', aux.clicodigo, aux.minano, aux.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', aux.clicodigo, aux.minano, aux.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', aux.clicodigo, aux.minano, aux.minnro, '@', coalesce(i.lotcodigo, 0) , '@', i.cmiitem))) as id_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante-proposta', aux.clicodigo, aux.minano, aux.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), aux.cmiid))) as id_gerado
	from (
		select
			participante.clicodigo,
			participante.minano,
			participante.minnro,
			item.cmiid ,
			participante.unicodigo
		from (
			select pl.clicodigo, pl.minano, pl.minnro, pl.unicodigo
			from wco.tbparlic pl
			where pl.prlhabilitacao <> 2
			and pl.clicodigo = 2016
			and pl.minano = 2020
			and pl.minnro = 128
		) participante
		cross join (
			select it.clicodigo, it.minano, it.minnro, it.cmiid
			from wco.tbitemin it
			where it.clicodigo = 2016
			and it.minano = 2020
			and it.minnro = 128
		) item
	) aux
	left join wco.tbitemin i on (i.clicodigo = aux.clicodigo and i.minano = aux.minano and i.minnro = aux.minnro and i.cmiid = aux.cmiid)
	inner join wun.tbunico u on (u.unicodigo = aux.unicodigo)
	where not exists (
		select 1
		from wco.tbcadqcp qcp
		where qcp.clicodigo = aux.clicodigo
		and qcp.minano = aux.minano
		and qcp.minnro = aux.minnro
		and qcp.unicodigo = aux.unicodigo
		and qcp.cmiid = aux.cmiid
	)
	and exists (
		select 1
		from wco.tbcadqcp qcp
		where qcp.clicodigo = aux.clicodigo
		and qcp.minano = aux.minano
		and qcp.minnro = aux.minnro
	)
	order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_processo is not null
and id_participante is not null