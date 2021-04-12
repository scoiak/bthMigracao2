select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-arp-sf' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_ata, '/', ano_ata) as ata,
	concat(nro_sf, '/', ano_sf, '(', clicodigo_origem, ')') as sf,
	'@' as separador,
	*
from (
	select
		a.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		a.arpano as ano_ata,
		a.arpnro as nro_ata,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		(case
			when c.copdataemissao < coalesce(a.arpdata, c.copdataemissao) then a.arpdata::varchar
			when c.copdataemissao > coalesce(a.arpdatavigfim, c.copdataemissao) then a.arpdatavigfim::varchar
			else c.copdataemissao::varchar
		end) as data_sf,
		--'2020-02-12' as data_sf,
		c.unicodigo,
		concat(c.copfinalidade, c.cophistorico, ' (Migração: Compra ', c.copnro, '/', c.copano, ', Ata ', a.arpnro, '/', a.arpano, ', Minuta ', c.minnro, '/', c.minano, ', Clicodigo Compra: ', c.clicodigo, ')') as observacao,
		c.coplocalentrega,
		c.cnccodigo,
		cc.cncclassif as nro_ogranograma,
		C.clicodigo as clicodigo_origem,
        coalesce((select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = c.usucodigo)), 'Não informado') as solicitante,		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_contratacao,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo',c.copano, replace(cc.cncclassif,'.','')))), 0) as id_organograma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', c.clicodigo))), upper(unaccent(left(coalesce(trim(c.coppreventrega),'Imediata'), 50)))))) as id_prazo_entrega,
		14011 as id_local_entrega,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf', a.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro))) as id_gerado
	from wco.tbcompra c
	inner join wco.tbataregpreco a on (a.clicodigo = coalesce(c.clicodigomin, c.clicodigo) and a.minano = c.minano and a.minnro = c.minnro and a.unicodigo = c.unicodigo)
	left join wun.tbcencus cc on (cc.organo = c.copano and cc.cnccodigo = c.cnccodigo)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where true
	and ((c.clicodigo = {{clicodigo}} and c.clicodigomin is null) or ((c.clicodigo = {{clicodigo}} and c.clicodigomin = c.clicodigo)) or (c.clicodigomin = {{clicodigo}} and c.clicodigo <> c.clicodigomin))
	and c.minano = {{ano}}
	and c.minnro = 119
	---and a.arpnro = 13
	and c.minano is not null
	and c.minnro is not null
	order by 1, 2 desc, 3 desc, 4 desc, 5 desc, 6 desc, 7 desc
) tab
where id_gerado is null
and id_contratacao is not null
and id_fornecedor is not null
--limit 1