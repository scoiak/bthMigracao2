select
	row_number() over() as id,
	'305' as sistema,
	'compra-direta' as tipo_registro,
	*
from (
	select
		c.clicodigo,
		c.copano as ano_cd,
		c.copnro as nro_cd,
		c.copano as ano_termo,
		c.copnro as nro_termo,
		c.copnro as sequencial,
		c.copdataemissao::varchar as data_assinatura,
		left(c.cophistorico, 500) as objeto,
		(select sum(icd.itcvlrtotal) from  wco.tbitemcompra icd where icd.clicodigo = c.clicodigo and icd.copano = c.copano and icd.copnro = c.copnro) as valor_original,
		'SEM_PROCESSO' as origem,
		true as fornecimento_imediato,
		'QUANTIDADE' as tipo_controle_saldo,
		'EXECUCAO' as situacao,
		concat('Compra direta ', c.copnro, '/', c.copano, '. ', c.copfinalidade) as observacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', c.copano))) as id_exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', c.clicodigo))) as id_entidade,
		10 as id_tipo_objeto, -- COMPRAS_SERVICOS
		26 as id_tipo_instrumento, -- SEM TERMO FORMAL
		48 as id_fundamento_legal, -- Lei 8666/93 Art.24, II
		u.unicpfcnpj,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		c.copcondpgto,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'forma-pagamento', coalesce(upper(unaccent(trim(c.copcondpgto))), 'Conforme Edital'))))  as id_forma_pagamento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'compra-direta', c.clicodigo, c.copano, c.copnro))) as id_gerado
	from wco.tbcompra c
	inner join wun.tbunico u on (u.unicodigo = c.unicodigo)
	where c.clicodigo = {{clicodigo}}
	and c.minano is null
	and c.minnro is null
	and c.copano >= {{ano}}
	order by 1, 2 desc, 3 asc
) tab
where id_gerado is null
and id_exercicio is not null
and id_entidade is not null
and id_fornecedor is not null
and id_forma_pagamento is not null
--limit 5