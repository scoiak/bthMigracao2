select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-aditivo-item' as tipo_registro,
	'@' as separador,
	*
from (
	select
		*,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', aux.clicodigo, aux.ano_contrato, aux.identificador_superior))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-item', aux.clicodigo, aux.ano_contrato, identificador_superior, '@', aux.cmiid))) as id_contratacao_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-aditivo', aux.clicodigo, aux.ano_contrato, nro_contrato))) as id_aditivo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-aditivo-item', aux.clicodigo, aux.ano_contrato, nro_contrato, '@', aux.cmiid))) as id_gerado
	from (
		select
			c.clicodigo,
			c.minano as ano_minuta,
			c.minnro as nro_minuta,
			'ITEM-ADITIVO' as origem,
			ct.ctrano as ano_contrato,
			ct.ctrnro as nro_contrato,
			c.ctridentificador as identificador_contrato,
			ct.ctridentsup as identificador_superior,
			c.copano as ano_sf,
			c.copnro as nro_sf,
			ic.cmiid,
			ic.itcvlrunit as valor_unitario,
			sum(ic.itcqtde) as quantidade,
			sum(ic.itcvlrtotal) as valor_total
		from wco.tbcompra c
		inner join wco.tbcontrato ct on (ct.clicodigoctl = c.clicodigo and ct.minano = c.minano and ct.minnro = c.minnro and ct.unicodigo = c.unicodigo and ct.ctridentificador = c.ctridentificador and ct.ctrtipoaditivo is not null)
		inner join wco.tbitemcompra ic on (ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro)
		left join wun.tbunico u on (u.unicodigo = c.unicodigo)
		where c.clicodigo = {{clicodigo}}
		and c.minano = {{ano}}
		and c.minnro in (1)
		and c.minano is not null
		and c.minnro is not null
		group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
		order by 1, 2 desc, 3 desc, 4 desc, 8 desc, 9 desc, 10 asc
	) aux
) tab
where id_gerado is null
and id_contratacao is not null
and id_aditivo is not null
and id_contratacao_item is not null
--limit 1