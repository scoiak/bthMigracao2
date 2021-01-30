select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-aditivo' as tipo_registro,
	concat(nro_aditivo, '/', ano_aditivo) as nro_aditivo,
	concat(nro_superior, '/', ano_contrato) as nro_contrato_superior,
	concat(nro_processo, '/', ano_processo) as nro_processo,
	*
from (
	select
		c.clicodigo,
		c.ctrano as ano_aditivo,
		c.ctridentificador as nro_aditivo,
		c.ctranosup as ano_contrato,
		c.ctridentsup as nro_superior,
		c.minano as ano_processo,
		c.minnro as nro_processo,
		c.ctrobjeto as objeto,
		c.ctrdatainivig::varchar as data_aditivo,
		c.ctrdatafimvig::varchar as data_final_nova,
		false as continua,
		false as reforma,
		c.ctrvalor as valor_aditivo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, c.ctrano, c.ctridentificador))) as id_contrato,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-aditivo', c.ctrtipoaditivo))) as id_tipo_aditivo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-aditivo', c.clicodigo, c.ctrano, c.ctridentificador))) as id_gerado
	from wco.tbcontrato c
	where c.clicodigo = {{clicodigo}}
	and c.ctrano = {{ano}}
	and c.ctrtipoaditivo is not null
) tab
where id_gerado is null
and id_tipo_aditivo is not null
and id_contrato is not null
--limit 1