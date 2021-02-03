select
	row_number() over() as id,
	'305' as sistema,
	'tipo-aditivo' as tipo_registro,
	*
from (
	select distinct
	   ctrtipoaditivo,
	   (case ctrtipoaditivo
	       when 1 then 'Normal'
		   when 2 then 'Objeto'
		   when 3 then 'Prazo'
		   when 4 then 'Valor (Equilibrio)'
		   when 5 then 'Prazo Valor'
		   when 6 then 'Objeto Valor'
		   when 7 then 'Objeto Prazo'
		   when 8 then 'Objeto Prazo Valor'
		   when 9 then 'Outros Aditivos'
		   when 10 then 'Recisão Contratual'
		   when 11 then 'Rerratificação'
		   when 12 then 'Apostila'
		   --when 13 then 'Apostila'
		   when 14 then 'Prorrogação'
		   else 'Cessão' end) as descricao,
	   (case ctrtipoaditivo
	       when 1 then 'OUTRAS_CLAUSULAS'
		   when 2 then 'OUTRAS_CLAUSULAS'
		   when 3 then 'PRAZO'
		   when 4 then 'REEQUILIBRIO_FINANCEIRO'
		   when 5 then 'PRAZO_ACRESCIMO'
		   when 6 then 'ACRESCIMO'
		   when 7 then 'PRAZO'
		   when 8 then 'PRAZO_ACRESCIMO'
		   when 9 then 'OUTRAS_CLAUSULAS'
		   when 10 then 'CESSAO_CONTRATUAL'
		   when 11 then 'OUTRAS_CLAUSULAS'
		   when 12 then 'REAJUSTE'
		   --when 13 then 'REAJUSTE'
		   when 14 then 'PRAZO'
		   else 'CESSAO_CONTRATUAL' end) as classificacao,
	   (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305,'tipo-aditivo', ctrtipoaditivo))) as id_gerado
	from wco.tbcontrato where ctrtipoaditivo is not null
) tab
where id_gerado is null
--limit 1