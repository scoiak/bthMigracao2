select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-sf-recebimento' as tipo_registro,
	false as houve_desconto,
	false as houve_retencao,
	coalesce((select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = tab.usucodigo limit 1)), 'Não informado') as responsavel,
	concat('(Migração: Recebimento da Compra ', copnro, '/', copano, ')') as observacao,
	'@' as separador,
	*
from (
	select distinct
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		nf.nfisequencia as numero,
		c.usucodigo,
		coalesce((select ct.ctranosup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador), c.ctrano) as exercicio,
		(case when coalesce(nf.nfidataentrega, nf.nfidataemissao, c.copdataemissao) < c.copdataemissao then c.copdataemissao::varchar else coalesce(nf.nfidataentrega, nf.nfidataemissao, c.copdataemissao)::varchar end) as data_provisoria,
		(case when coalesce(nf.nfidataentrega, nf.nfidataemissao, c.copdataemissao) < c.copdataemissao then c.copdataemissao::varchar else coalesce(nf.nfidataentrega, nf.nfidataemissao, c.copdataemissao)::varchar end) as data_definitiva,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', nf.clicodigo, (select ct.ctranosup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador), (select ct.ctridentsup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador)))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where tipo_registro = 'contratacao-sf' and i_chave_dsk1 = c.clicodigoctr::varchar and i_chave_dsk5 = c.copano::varchar and i_chave_dsk6 = c.copnro::varchar) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-recebimento', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbminuta t using (clicodigo, minano, minnro)
	where nf.clicodigo = {{clicodigo}}
	and t.minano = {{ano}}
	and t.minnro = 47
	--and c.copnro in (1523, 1522)
	and c.minano is not null
	and c.minnro is not null
	and t.mintipoconcorrencia <> 2
	and exists (select 1
				from wco.tbitemnota inf
				where inf.clicodigo = nf.clicodigo
				and inf.copano = nf.copano
				and inf.copnro = nf.copnro
				and inf.nfisequencia = nf.nfisequencia
				limit 1)
) tab
where id_gerado is null
and id_sf is not null
and id_contratacao is not null