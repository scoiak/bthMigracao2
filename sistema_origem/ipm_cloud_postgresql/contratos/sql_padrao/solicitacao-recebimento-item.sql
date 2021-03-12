select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao-recebimento-item' as tipo_registro,
	'@' as separador,
	*
from (
	(select
		'compra_direta' as origem,
		i.clicodigo,
		i.copano,
		i.copnro,
		i.nfisequencia,
		i.itcitem,
		i.itnqtde as quantidade,
		0.00 as desconto,
		i.itnvalortotal as valor_total,
		i.copano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', i.clicodigo, i.copano, i.copnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-sf', i.clicodigo, i.copano, i.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-sf-item', i.clicodigo, i.copano, i.copnro, '@', i.itcitem))) as id_sf_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento-item', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia, '@', i.itcitem))) as id_gerado
	from wco.tbitemnota i
	where i.clicodigo = {{clicodigo}}
	and i.copano = {{ano}})
union all
	(select
		'processo' as origem,
		i.clicodigo,
		i.copano,
		i.copnro,
		i.nfisequencia,
		i.itcitem,
		i.itnqtde as quantidade,
		0.00 as desconto,
		i.itnvalortotal as valor_total,
		ct.ctrano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao',  ct.clicodigo, ct.ctrano, coalesce(ct.ctridentsup, ct.ctridentificador)))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-item', i.clicodigo, i.copano, i.copnro, '@', i.itcitem))) as id_sf_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento-item', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia, '@', i.itcitem))) as id_gerado
	from wco.tbitemnota i
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbminuta t using (clicodigo, minano, minnro)
	inner join wco.tbcontrato ct using (clicodigo, minano, minnro, unicodigo)
	where i.clicodigo = {{clicodigo}}
	and i.copano = {{ano}}
	and c.minano is not null
	and c.minnro is not null
	and ct.ctridentificador is not null
	and t.mintipoconcorrencia <> 2)
union all
	(select distinct
		'arp' as origem,
		i.clicodigo,
		i.copano,
		i.copnro,
		i.nfisequencia,
		i.itcitem,
		i.itnqtde as quantidade,
		0.00 as desconto,
		i.itnvalortotal as valor_total,
		a.arpano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf', a.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf-item', a.clicodigo, a.arpano, a.arpnro, '@', i.copano, i.copnro, '@', i.itcitem))) as id_sf_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento-item', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia, '@', ic.cmiid))) as id_gerado
	from wco.tbitemnota i
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbitemcompra ic using (clicodigo, copano, copnro, itcitem)
	inner join wco.tbataregpreco a on (a.clicodigo = coalesce(c.clicodigomin, c.clicodigo) and a.minano = c.minano and a.minnro = c.minnro and a.unicodigo = c.unicodigo)
	where i.clicodigo = {{clicodigo}}
	and i.copano = {{ano}}
	and c.minano is not null
	and c.minnro is not null)
) tab
where id_gerado is null
and id_contratacao is not null
and id_sf is not null
and id_sf_item is not null
and id_recebimento is not null
--limit 1