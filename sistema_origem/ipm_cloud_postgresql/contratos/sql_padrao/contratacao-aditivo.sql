select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-aditivo' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_superior, '/', ano_contrato) as contratacao,
	concat(nro_aditivo, '/', ano_aditivo, ' (', identificador_aditivo, ')') as aditivo,
	*
from (
	select
		c.clicodigo,
		c.ctranosup as ano_contrato,
		c.ctridentsup as nro_superior,
		c.ctrdatainivig::varchar as data_aditivo,
		(case when c.ctrtipoaditivo in (5, 7, 8, 14) then c.ctrdatafimvig::varchar else null end) data_final_nova,
		c.ctrdataassinatura::varchar as data_assinatura,
		c.adjsequencia,
		(case
			when c.ctrtipoaditivo not in (4, 5, 6, 7, 8) then c.ctrvalor
			when c.ctrtipoaditivo in (4, 5, 6, 7, 8) and c.ctrvalor = 0 then (select sum(itcvlrtotal) from wco.tbitemcompra ic where ic.clicodigo = c.clicodigo and ic.minano = c.minano and ic.minnro = c.minnro and ic.adjsequencia = c.adjsequencia)
			else c.ctrvalor
		end) as valor_aditivo,
		c.ctrano as ano_aditivo,
		c.ctrnro as nro_aditivo,
		c.ctridentificador as identificador_aditivo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		concat(c.ctrobjeto, ' (Migração: Aditivo ', c.ctrnro, '/', c.ctrano, ', Identificador ', c.ctridentificador, ', Minuta ', c.minnro, '/', c.minano, ')') as objeto,
		c.ctrtipoaditivo,
		(case c.ctrtipoaditivo when 1 then 'Normal' when 2 then 'Objeto' when 3 then 'Prazo' when 4 then 'Valor (Equilibrio)' when 5 then 'Prazo Valor'when 6 then 'Objeto Valor' when 7 then 'Objeto Prazo' when 8 then 'Objeto Prazo Valor' when 9 then 'Outros Aditivos' when 10 then 'Recisão Contratual' when 11 then 'Rerratificação' when 12 then 'Apostila' when 13 then '???' when 14 then 'Prorrogação' else 'Cessão' end) as desc_tipo_aditivo,
		false as continua,
		false as reforma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, c.ctranosup, c.ctridentsup))) as id_contrato,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-aditivo', c.ctrtipoaditivo))) as id_tipo_aditivo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-aditivo', c.clicodigo, c.ctrano, c.ctrnro))) as id_gerado
	from wco.tbcontrato c
	where c.clicodigo = {{clicodigo}}
	and c.ctranosup = {{ano}}
	and c.ctrtipoaditivo is not null
	and c.ctrtipoaditivo <> 12
	and c.ctridentsup = 51
	order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_tipo_aditivo is not null
and id_contrato is not null
--limit 1
