select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao-recebimento-comprovante' as tipo_registro,
	concat(copnro, '/', copano) as nro_sf,
	'@' as separador,
	*
from (
	(select distinct
		'compra_direta' as origem,
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		nf.nfisequencia,
		coalesce((select sum(inf.itnvalortotal) from wco.tbitemnota inf where inf.clicodigo = nf.clicodigo and inf.copano = nf.copano and inf.copnro = nf.copnro and inf.nfisequencia = nf.nfisequencia), 0) as valor,
		nf.copano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_comprovante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', nf.clicodigo, nf.copano, nf.copnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-sf', nf.clicodigo, nf.copano, nf.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento-comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	where nf.clicodigo = {{clicodigo}}
	and nf.copano = {{ano}}
	and c.minano is null
	and c.minnro is null)
union all
	(select distinct
		'processo' as origem,
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		nf.nfisequencia,
		coalesce((select sum(inf.itnvalortotal) from wco.tbitemnota inf where inf.clicodigo = nf.clicodigo and inf.copano = nf.copano and inf.copnro = nf.copnro and inf.nfisequencia = nf.nfisequencia), 0) as valor,
		ct.ctrano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_comprovante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao',  ct.clicodigo, ct.ctrano, coalesce(ct.ctridentsup, ct.ctridentificador)))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento-comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbminuta t using (clicodigo, minano, minnro)
	inner join wco.tbcontrato ct using (clicodigo, minano, minnro, unicodigo)
	where nf.clicodigo = {{clicodigo}}
	and nf.copano = {{ano}}
	and ct.ctridentificador is not null
	and t.mintipoconcorrencia <> 2
	and c.minano is not null
	and c.minnro is not null)
union all
	(select distinct
		'arp' as origem,
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		nf.nfisequencia,
		coalesce((select sum(inf.itnvalortotal) from wco.tbitemnota inf where inf.clicodigo = nf.clicodigo and inf.copano = nf.copano and inf.copnro = nf.copnro and inf.nfisequencia = nf.nfisequencia), 0) as valor,
		a.arpano as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_comprovante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf', a.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro))) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'solicitacao-recebimento-comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbataregpreco a on (a.clicodigo = coalesce(c.clicodigomin, c.clicodigo) and a.minano = c.minano and a.minnro = c.minnro and a.unicodigo = c.unicodigo)
	where nf.clicodigo = {{clicodigo}}
	and nf.copano = {{ano}}
	and c.minano is not null
	and c.minnro is not null)
) tab
where id_gerado is null
and id_comprovante is not null
and id_contratacao is not null
and id_sf is not null
and id_recebimento is not null
--limit 1