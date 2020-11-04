select *,ROW_NUMBER() OVER()::varchar as id,ROW_NUMBER() OVER()::varchar as codigo from (select
'Configuração Geral' as descricao,
            'true' as emUso,
                (select string_agg(nivel::varchar || '%|%' || descricao || '%|%' || quantidadeDigitos::varchar || '%|%' || separador,'%||%') from
	                    (select
	                    1 as nivel,
	                    'Código' as descricao,
	                    3 as quantidadeDigitos,
	                    'PONTO' as separador
	                ) as suc) as niveis
	                    ) as a