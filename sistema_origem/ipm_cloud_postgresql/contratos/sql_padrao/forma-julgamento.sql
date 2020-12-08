select
	row_number() over() as id,
	'305' as sistema,
	'forma-julgamento' as tipo_registro,
	*
from (
	select distinct
       clicodigo as chave_dsk1,
       mintipojulgamento as chave_dsk2,
       mintipocomparacao as chave_dsk3,
       clicodigo,
       (case
       		when ((mintipojulgamento = 1 or mintipojulgamento is null) and mintipocomparacao = 1) then 'Menor Preço por Item'
       		when ((mintipojulgamento = 1 or mintipojulgamento is null) and mintipocomparacao = 2) then 'Menor Preço por Global'
       		when ((mintipojulgamento = 1 or mintipojulgamento is null) and mintipocomparacao = 3) then 'Menor Preço por Lote'
       		when ((mintipojulgamento = 4 or mintipojulgamento is null) and mintipocomparacao = 1) then 'Maior Lance ou Oferta por Item'
       		when ((mintipojulgamento = 4 or mintipojulgamento is null) and mintipocomparacao = 2) then 'Maior Lance ou Oferta Global'
       		when ((mintipojulgamento = 4 or mintipojulgamento is null) and mintipocomparacao = 3) then 'Maior Lance ou Oferta por Lote'
       		else null end) as descricao,
       (case
       		when mintipojulgamento = 1 or mintipojulgamento is null then 'MENOR_PRECO'
			else 'MELHOR_LANCE_OFERTA' end) as tipoLicitacao,
       (case mintipocomparacao
            when 1 then 'ITEM'
			when 2 then 'GLOBAL'
            when 3 then 'LOTE'
            end) as tipoJulgamento,
       'NENHUMA' as formaEspecial,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'forma-julgamento', clicodigo, mintipojulgamento, mintipocomparacao))) as id_gerado
	from wco.tbminuta
	where mintipojulgamento is not null
) tab
where id_gerado is null
and descricao is not null
and tipojulgamento is not null
and clicodigo = {{clicodigo}}