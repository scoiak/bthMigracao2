select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-sf' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_contrato, '/', ano_contrato, ' (', identificador_contrato, ')') as contrato,
	concat(nro_sf, '/', ano_sf) as sf,
	'@' as separador,
	*
from (
	(select distinct
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		c.copdataemissao::varchar as data_sf,
		'PRINCIPAL' as origem,
		ct.ctrano as ano_contrato,
		ct.ctrnro as nro_contrato,
		ct.ctridentificador as identificador_contrato,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.unicodigo,
		left(concat(c.copfinalidade, c.cophistorico, ' (Migração: Compra ', c.copnro, '/', c.copano, ', Contrato ', ct.ctrnro, '/', ct.ctrano, ', Identificador ',  ct.ctridentificador, ', Minuta ', c.minnro, '/', c.minano, ').'), 2048) as observacao,
		(select sum(itcqtde) from wco.tbitemcompra ic where ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro) as qtd_comprada,
		coalesce((select sum(iec.iesqtde) from wco.tbitemcompraest iec where iec.clicodigo = c.clicodigo and iec.copano = c.copano and iec.copnro = c.copnro), 0) as qtd_estornada,
		c.coplocalentrega,
		c.cnccodigo,
		cc.cncclassif as nro_ogranograma,
		coalesce((select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = c.usucodigo)), 'Migração') as solicitante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', ct.clicodigo, ct.ctrano, ct.ctridentificador))) as id_contratacao,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo', c.copano, replace(cc.cncclassif,'.','')))), 0) as id_organograma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', c.clicodigo))), upper(unaccent(left(coalesce(trim(c.coppreventrega),'Imediata'), 50)))))), 0) as id_prazo_entrega,
		14011 as id_local_entrega,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_gerado
	from wco.tbcompra c
	inner join wco.tbcontrato ct on (ct.clicodigo = c.clicodigoctr and ct.ctrano = c.ctrano and ct.ctridentificador = c.ctridentificador and ct.ctrtipoaditivo is null)
	left join wun.tbcencus cc on (cc.organo = c.copano and cc.cnccodigo = c.cnccodigo)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where true
	and c.clicodigo = {{clicodigo}}
	and c.clicodigomin = c.clicodigo
	and c.minano = {{ano}}
	--and c.minnro in (203)
	and c.minano is not null
	and c.minnro is not null
	and not exists (select 1 from wco.tbataregpreco a where a.clicodigo  = c.clicodigo and a.minano = c.minano and a.minnro = c.minnro)
	--and c.copnro not in (select aux.copnro from wco.tbcompra aux where aux.clicodigoctr = c.clicodigomin and aux.minano = c.minano and aux.minnro = c.minnro)
	order by 1, 2 desc, 3 desc, 4 asc, 5 desc, 6 desc, 7 desc)
union all
	(select distinct
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		c.copdataemissao::varchar as data_sf,
		'ADITIVO' as origem,
		ct.ctrano as ano_contrato,
		ct.ctrnro as nro_contrato,
		ct.ctridentificador as identificador_contrato,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.unicodigo,
		left(concat(c.copfinalidade, c.cophistorico, ' (Migração: Compra ', c.copnro, '/', c.copano, ', Contrato ', ct.ctrnro, '/', ct.ctrano, ', Identificador ',  ct.ctridentificador, ', Minuta ', c.minnro, '/', c.minano, ').'), 2048) as observacao,
		(select sum(itcqtde) from wco.tbitemcompra ic where ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro) as qtd_comprada,
		coalesce((select sum(iec.iesqtde) from wco.tbitemcompraest iec where iec.clicodigo = c.clicodigo and iec.copano = c.copano and iec.copnro = c.copnro), 0) as qtd_estornada,
		c.coplocalentrega,
		c.cnccodigo,
		cc.cncclassif as nro_ogranograma,
		coalesce((select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = c.usucodigo)), 'Migração') as solicitante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', coalesce(ct.clicodigosup, ct.clicodigo), coalesce(ct.ctranosup, ct.ctrano), coalesce(ct.ctridentsup, ct.ctridentificador)))) as id_contratacao,
		--882285 as id_contratacao,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo', c.copano, replace(cc.cncclassif,'.','')))), 0) as id_organograma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', c.clicodigo))), upper(unaccent(left(coalesce(trim(c.coppreventrega),'Imediata'), 50)))))), 0) as id_prazo_entrega,
		14011 as id_local_entrega,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_gerado
	from wco.tbcompra c
	inner join wco.tbcontrato ct on (ct.clicodigo = c.clicodigoctr and ct.ctrano = c.ctrano and ct.ctridentificador = c.ctridentificador and ct.ctrtipoaditivo is not null)
	left join wun.tbcencus cc on (cc.organo = c.copano and cc.cnccodigo = c.cnccodigo)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where true
	and c.clicodigo = {{clicodigo}}
	and c.clicodigomin = c.clicodigo
	and c.minano = {{ano}}
	--and c.minnro in (203)
	and c.minano is not null
	and c.minnro is not null
	and not exists (select 1 from wco.tbataregpreco a where a.clicodigo  = c.clicodigo and a.minano = c.minano and a.minnro = c.minnro)
	--and c.copnro not in (select aux.copnro from wco.tbcompra aux where aux.clicodigoctr = c.clicodigomin and aux.minano = c.minano and aux.minnro = c.minnro)
	order by 1, 2 desc, 3 desc, 4 asc, 5 desc, 6 desc, 7 desc)
union all
	(select distinct
		c.clicodigo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		c.copdataemissao::varchar as data_sf,
		'PRINCIPAL_SEM_CONTRATO' as origem,
		ct.ctrano as ano_contrato,
		ct.ctrnro as nro_contrato,
		ct.ctridentificador as identificador_contrato,
		c.copano as ano_sf,
		c.copnro as nro_sf,
		c.unicodigo,
		left(concat(c.copfinalidade, c.cophistorico, ' (Migração: Compra ', c.copnro, '/', c.copano, ', Sem vínculo Contrato,  Identificador ',  ct.ctridentificador, ', Minuta ', c.minnro, '/', c.minano, ').'), 2048) as observacao,
		(select sum(itcqtde) from wco.tbitemcompra ic where ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro) as qtd_comprada,
		coalesce((select sum(iec.iesqtde) from wco.tbitemcompraest iec where iec.clicodigo = c.clicodigo and iec.copano = c.copano and iec.copnro = c.copnro), 0) as qtd_estornada,
		c.coplocalentrega,
		c.cnccodigo,
		cc.cncclassif as nro_ogranograma,
		coalesce((select u.uninomerazao from wun.tbunico u where u.unicodigo = (select usr.unicodigo from webbased.tbusuario usr where usr.usucodigo = c.usucodigo)), 'Migração') as solicitante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', coalesce(ct.clicodigosup, ct.clicodigo), coalesce(ct.ctranosup, ct.ctrano), coalesce(ct.ctridentsup, ct.ctridentificador)))) as id_contratacao,
		--882285 as id_contratacao,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo', c.copano, replace(cc.cncclassif,'.','')))), 0) as id_organograma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', c.clicodigo))), upper(unaccent(left(coalesce(trim(c.coppreventrega),'Imediata'), 50)))))), 0) as id_prazo_entrega,
		14011 as id_local_entrega,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf', c.clicodigo, ct.ctrano, ct.ctridentificador, '@', c.copano, c.copnro))) as id_gerado
	from wco.tbcompra c
	left join wco.tbcontrato ct on (ct.clicodigo = c.clicodigo and ct.minano = c.minano and ct.minnro = c.minnro and ct.unicodigo = c.unicodigo and ct.ctrtipoaditivo is null)
	left join wun.tbcencus cc on (cc.organo = c.copano and cc.cnccodigo = c.cnccodigo)
	left join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where true
	and c.clicodigo = {{clicodigo}}
	and c.clicodigomin = c.clicodigo
	and c.minano = {{ano}}
	--and c.minnro in (203)
	and c.minano is not null
	and c.minnro is not null
	and c.ctrano is null
	and c.ctridentificador is null
	and not exists (select 1 from wco.tbataregpreco a where a.clicodigo  = c.clicodigo and a.minano = c.minano and a.minnro = c.minnro)
	--and c.copnro not in (select aux.copnro from wco.tbcompra aux where aux.clicodigoctr = c.clicodigomin and aux.minano = c.minano and aux.minnro = c.minnro)
	order by 1, 2 desc, 3 desc, 4 asc, 5 desc, 6 desc, 7 desc)
) tab
where id_gerado is null
and id_contratacao is not null
and id_fornecedor is not null
--and origem in ('PRINCIPAL', 'PRINCIPAL_SEM_CONTRATO')
--limit 1