select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-arp-sf' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_ata, '/', ano_ata) as ata,
	concat(nro_sf, '/', ano_sf) as sf,
	'@' as separador,
	*
from (
	select
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		a.arpano as ano_ata,
		a.arpnro as nro_ata,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.copdataemissao::varchar as data_sf,
		c.unicodigo,
		concat(c.copfinalidade, c.cophistorico, ' (Migração: Compra ', c.copnro, '/', c.copano, ', Ata ', a.arpnro, '/', a.arpano, ', Minuta ', c.minnro, '/', c.minano, ')') as observacao,
		c.coplocalentrega,
		c.cnccodigo,
		cc.cncclassif as nro_ogranograma,
		(select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = c.usucodigo)) as solicitante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo',c.copano, replace(cc.cncclassif,'.','')))) as id_organograma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', c.clicodigo))), upper(unaccent(left(coalesce(trim(c.coppreventrega),'Imediata'), 50)))))) as id_prazo_entrega,
		14011 as id_local_entrega,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp-sf', c.clicodigo, a.arpano, a.arpnro, '@', c.copano, c.copnro))) as id_gerado
	from wco.tbcompra c
	inner join wco.tbataregpreco a on (a.clicodigo = c.clicodigo and a.minano = c.minano and a.minnro = c.minnro and a.unicodigo = c.unicodigo)
	left join wun.tbcencus cc on (cc.organo = c.copano and cc.cnccodigo = c.cnccodigo)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where c.clicodigo = {{clicodigo}}
	and c.minano = {{ano}}
	and c.minnro = 103
	--and a.arpnro = 69
	and c.minano is not null
	and c.minnro is not null
	order by 1, 2 desc, 3 desc, 4 desc, 5 desc, 6 desc, 7 desc
) tab
where id_gerado is null
and id_contratacao is not null
and id_organograma is not null
and id_fornecedor is not null
--limit 1