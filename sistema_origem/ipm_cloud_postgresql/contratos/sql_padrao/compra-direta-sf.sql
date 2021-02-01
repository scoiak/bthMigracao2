select
	row_number() over() as id,
	'305' as sistema,
	'compra-direta-sf' as tipo_registro,
	concat(nro_sf, '/', ano_sf) as contratacao,
	concat(nro_sf, '/', ano_sf) as sf,
	*
from (
	select
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.copdataemissao::varchar as data_sf,
		concat(c.copfinalidade, c.cophistorico, ' (Migração: ', c.copnro, '/', c.copano, ').') as observacao,
		c.coplocalentrega,
		c.cnccodigo,
		c.unicodigo,
		cc.cncclassif as nro_ogranograma,
		(select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = c.usucodigo)) as solicitante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', c.clicodigo, c.copano, c.copnro))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo',c.copano, replace(cc.cncclassif,'.','')))) as id_organograma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', c.clicodigo))), upper(unaccent(left(coalesce(trim(c.coppreventrega),'Imediata'), 50)))))) as id_prazo_entrega,
		14011 as id_local_entrega,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta-sf', c.clicodigo, c.copano, c.copnro))) as id_gerado
	from wco.tbcompra c
	left join wun.tbcencus cc on (cc.organo = c.copano and cc.cnccodigo = c.cnccodigo)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where c.clicodigo = {{clicodigo}}
	and c.copano = {{ano}}
	and c.minano is null
	and c.minnro is null
	order by 1, 4 desc, 5 desc
) tab
where id_gerado is null
and id_contratacao is not null
and id_organograma is not null
and id_fornecedor is not null
limit 1