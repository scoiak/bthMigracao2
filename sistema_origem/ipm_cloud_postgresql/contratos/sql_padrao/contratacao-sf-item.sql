select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-sf-item' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_contrato, '/', ano_contrato, ' (', identificador_contrato, ')') as contratacao,
	concat(nro_sf, '/', ano_sf) as sf,
	'@' as separador,
	*
from (
	(select
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		'ITEM-PRINCIPAL' as origem,
		ct.ctrano as ano_contrato,
		ct.ctrnro as nro_contrato,
		ct.ctridentificador as identificador_contrato,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		ic.cmiid,
		ic.itcqtde as quantidade,
		ic.itcvlrunit as valor_unitario,
		ic.itcvlrtotal as valor_total,
		ct.ctrdataassinatura,
		coalesce((select sum(e.iesqtde) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0) as qtd_estorno,
		coalesce((select sum(e.iesvlrtotal) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0) as valor_estorno,
		(ic.itcqtde - coalesce((select sum(e.iesqtde) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0)) as qtd_liquida,
		(ic.itcvlrtotal - coalesce((select sum(e.iesvlrtotal) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0)) as valor_liquido,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, ct.ctrano, ct.ctridentificador))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_solicitacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-item', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', ic.cmiid))) as id_contratacao_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', ic.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', ic.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-item', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro, '@', ic.cmiid))) as id_gerado
	from wco.tbcompra c
	inner join wco.tbcontrato ct on (ct.clicodigo = c.clicodigoctr and ct.ctrano = c.ctrano and ct.ctridentificador = c.ctridentificador and ct.ctrtipoaditivo is null)
	inner join wco.tbitemcompra ic on (ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where c.clicodigomin = {{clicodigo}}
	and c.clicodigo = c.clicodigomin
	and c.minano = {{ano}}
	and c.minnro = 85
	and c.minano is not null
	and c.minnro is not null
	order by 1, 2 desc, 3 desc, 4 desc, 8 desc, 9 desc, 10 asc)
union all
	(select
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		'ITEM-ADITIVO' as origem,
		ct.ctrano as ano_contrato,
		ct.ctrnro as nro_contrato,
		c.ctridentificador as identificador_contrato,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		ic.cmiid,
		ic.itcqtde as quantidade,
		ic.itcvlrunit as valor_unitario,
		ic.itcvlrtotal as valor_total,
		ct.ctrdataassinatura,
		coalesce((select sum(e.iesqtde) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0) as qtd_estorno,
		coalesce((select sum(e.iesvlrtotal) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0) as valor_estorno,
		(ic.itcqtde - coalesce((select sum(e.iesqtde) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0)) as qtd_liquida,
		(ic.itcvlrtotal - coalesce((select sum(e.iesvlrtotal) from wco.tbitemcompraest e where e.clicodigo = ic.clicodigo and e.copano = ic.copano and e.copnro = ic.copnro and e.itcitem = ic.itcitem), 0)) as valor_liquido,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, ct.ctranosup, ct.ctridentsup))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, c.ctrano, c.ctridentificador, '@', c.copano, c.copnro))) as id_solicitacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-item', c.clicodigo, ct.ctranosup , ct.ctridentsup, '@', ic.cmiid))) as id_contratacao_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', ic.prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', ic.prdcodigo))) as id_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-item', ct.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro, '@', ic.cmiid))) as id_gerado
	from wco.tbcompra c
	inner join wco.tbcontrato ct on (ct.clicodigo = c.clicodigoctr and ct.ctrano = c.ctrano and ct.ctridentificador = c.ctridentificador and ct.ctrtipoaditivo is not null)
	inner join wco.tbitemcompra ic on (ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where c.clicodigomin = {{clicodigo}}
	and c.clicodigo = c.clicodigomin
	and c.minano = {{ano}}
	and c.minnro = 85
	--and c.copnro = 1687
	and c.minano is not null
	and c.minnro is not null
	order by 1, 2 desc, 3 desc, 4 desc, 8 desc, 9 desc, 10 asc)
) tab
where id_gerado is null
and id_solicitacao is not null
and id_contratacao_item is not null
and id_material is not null
and id_especificacao is not null
and qtd_liquida > 0 -- Impede o envio de itens totalmente estornados/anulados
--and origem = 'ITEM-PRINCIPAL'
--limit 10