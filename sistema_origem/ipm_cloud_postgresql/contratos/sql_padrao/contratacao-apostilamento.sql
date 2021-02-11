select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-apostilamento' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_superior, '/', ano_contrato, ' (', identificador_superior, ')') as contratacao,
	concat(nro_aditivo, '/', ano_aditivo, ' (', identificador_aditivo, ')') as aditivo,
	*
from (
	select
		c.clicodigo,
		c.ctranosup as ano_contrato,
		(select auxc.ctrnro from wco.tbcontrato auxc where auxc.clicodigo = c.clicodigo and auxc.ctrano = c.ctranosup and auxc.ctridentificador = c.ctridentsup) as nro_superior,
		c.ctridentsup as identificador_superior,
		c.ctrdatainivig::varchar as data_aditivo,
		c.ctrdataassinatura::varchar as data_assinatura,
		--'2018-07-10' as data_assinatura,
		c.ctrano as ano_aditivo,
		c.ctrnro as nro_aditivo,
		c.ctridentificador as identificador_aditivo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		c.ctrdatafimvig::varchar as data_final_nova,
		concat('Alteração de Despesa Orçamentária', ' (Migração: Aditivo ', c.ctrnro, '/', c.ctrano, ', Identificador ', c.ctridentificador, ', Minuta ', c.minnro, '/', c.minano, ')') as descricao,
		c.ctrtipoaditivo,
		'SEM_ALTERACAO' as tipo_alteracao,
		99 as sequencial,
		2 as id_tipo_apostilamento, --Alteração de Despesa Orçamentária
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, c.ctranosup, c.ctridentsup))) as id_contrato,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-aditivo', c.ctrtipoaditivo))) as id_tipo_aditivo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-apostilamento', c.clicodigo, c.ctrano, c.ctrnro))) as id_gerado
	from wco.tbcontrato c
	where c.clicodigoctl = {{clicodigo}}
	--and c.ctranosup = {{ano}}
	and c.minano = {{ano}}
	and c.minnro = 258
	and c.ctrtipoaditivo is not null
	and c.ctrtipoaditivo = 12
	--and c.minnro = 2
	--and c.ctrnro = 'Apostila 5'
	order by 1, 2 desc, 3 desc, 6 asc
) tab
where id_gerado is null
and id_contrato is not null
and id_tipo_aditivo is not null
--limit 1