select
	row_number() over() as id,
	'305' as sistema,
	'regime-execucao' as tipo_registro,
	*
from (
	select distinct
       (case mintiporegexec
          when 1 then 'Empreitada por Preço Global'
          when 2 then 'Empreitada por Preço Unitário'
          when 3 then 'Empreitada por Preço Global Integral'
          when 4 then 'Tarefa'
          when 5 then 'Execução Direta'
          when 6 then 'Cessão de Direitos'
          when 7 then 'Serviços'
          when 8 then 'Alienação de Bens Móveis'
          when 9 then 'Alienação de Bens Imóveis'
          when 10 then 'Cessão de Direitos'
          when 11 then 'Cessão de Direito Real de Uso - Bens Públicos'
          else 'Compras' end) as descricao,
       (case mintiporegexec
          when 1 then 'EXECUCAO_INDIRETA_GLOBAL'
          when 2 then 'EXECUCAO_INDIRETA_PRECO_UNITARIO'
          when 3 then 'EXECUCAO_INDIRETA_EMPREITADA_INTEGRAL'
          when 4 then 'TAREFA'
          when 5 then 'EXECUCAO_DIRETA'
          when 6 then 'CESSAO_DIREITOS'
          when 7 then 'SERVICOS'
          when 8 then 'ALIENACAO_BENS'
          when 9 then 'ALIENACAO_BENS'
          when 10 then 'CESSAO_DIREITOS'
          when 11 then 'CONCESSAO_DIREITO_REAL_USO'
          else 'COMPRAS' end) as tipo,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'regime-execucao', (case mintiporegexec
																													          when 1 then 'Empreitada por Preço Global'
																													          when 2 then 'Empreitada por Preço Unitário'
																													          when 3 then 'Empreitada por Preço Global Integral'
																													          when 4 then 'Tarefa'
																													          when 5 then 'Execução Direta'
																													          when 6 then 'Cessão de Direitos'
																													          when 7 then 'Serviços'
																													          when 8 then 'Alienação de Bens Móveis'
																													          when 9 then 'Alienação de Bens Imóveis'
																													          when 10 then 'Cessão de Direitos'
																													          when 11 then 'Cessão de Direito Real de Uso - Bens Públicos'
																													          else 'Compras' end)))) as id_gerado
  from wco.tbminuta
  where mintiporegexec is not null
) tab
where id_gerado is null