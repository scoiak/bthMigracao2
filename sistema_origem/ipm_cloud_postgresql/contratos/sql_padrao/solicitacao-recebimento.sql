select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao-recebimento' as tipo_registro,
	1 as numero,
	false as houve_desconto,
	false as houve_retencao,
	coalesce((select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = tab.usucodigo limit 1)), 'Não informado') as responsavel,
	concat('(Migração: Recebimento da Compra ', copnro, '/', copano, ')') as observacao,
	*
from (
	(select distinct
		'compra_direta' as origem,
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		c.usucodigo,
		coalesce(nf.nfidataentrega, c.copdataemissao)::varchar as data_provisoria,
		coalesce(nf.nfidataentrega, c.copdataemissao)::varchar as data_definitiva,
		nf.copano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', nf.clicodigo, nf.copano, nf.copnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-sf',nf.clicodigo, nf.copano, nf.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', nf.clicodigo, nf.copano, nf.copnro))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	where nf.clicodigo = {{clicodigo}}
	and nf.copano = {{ano}}
	and c.minano is null
	and c.minnro is null
	and exists (select 1
				from wco.tbitemnota inf
				where inf.clicodigo = nf.clicodigo
				and inf.copano = nf.copano
				and inf.copnro = nf.copnro
				and inf.nfisequencia = nf.nfisequencia
				limit 1))
union all
	(select distinct
		'processo' as origem,
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		c.usucodigo,
		coalesce(nf.nfidataentrega, c.copdataemissao)::varchar as data_provisoria,
		coalesce(nf.nfidataentrega, c.copdataemissao)::varchar as data_definitiva,
		ct.ctrano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao',  ct.clicodigo, ct.ctrano, coalesce(ct.ctridentsup, ct.ctridentificador)))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', nf.clicodigo, nf.copano, nf.copnro))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbminuta t using (clicodigo, minano, minnro)
	inner join wco.tbcontrato ct using (clicodigo, minano, minnro, unicodigo)
	where nf.clicodigo = {{clicodigo}}
	and nf.copano = {{ano}}
	and c.minano is not null
	and c.minnro is not null
	and ct.ctridentificador is not null
	and t.mintipoconcorrencia <> 2
	and exists (select 1
				from wco.tbitemnota inf
				where inf.clicodigo = nf.clicodigo
				and inf.copano = nf.copano
				and inf.copnro = nf.copnro
				and inf.nfisequencia = nf.nfisequencia
				limit 1))
union all
	(select distinct
		'arp' as origem,
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		c.usucodigo,
		coalesce(nf.nfidataentrega, c.copdataemissao)::varchar as data_provisoria,
		coalesce(nf.nfidataentrega, c.copdataemissao)::varchar as data_definitiva,
		a.arpano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf', a.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', nf.clicodigo, nf.copano, nf.copnro))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbataregpreco a on (a.clicodigo = coalesce(c.clicodigomin, c.clicodigo) and a.minano = c.minano and a.minnro = c.minnro and a.unicodigo = c.unicodigo)
	where nf.clicodigo = {{clicodigo}}
	and nf.copano = {{ano}}
	and c.minano is not null
	and c.minnro is not null
	and exists (select 1
				from wco.tbitemnota inf
				where inf.clicodigo = nf.clicodigo
				and inf.copano = nf.copano
				and inf.copnro = nf.copnro
				and inf.nfisequencia = nf.nfisequencia
				limit 1))
) tab
where id_gerado is null
and id_contratacao is not null
and id_sf is not null
and origem in ('compra_direta', 'processo', 'arp')
--and copnro = 182
limit 1