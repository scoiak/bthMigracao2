select  
		ROW_NUMBER() OVER()::varchar as id,
		ROW_NUMBER() OVER()::varchar as codigo,
		'Configuração Geral Organograma' as descricao,
		*
		from (select
            'true' as emUso,
                (select string_agg(nivel::varchar || '%|%' || descricao || '%|%' || quantidadeDigitos::varchar || '%|%' || separador || '%|%' || responsavelControleVagas || '%|%' || nivelSecretaria,'%||%') from
	                    (				 select 1 as nivel,
	                    'Ano' as descricao,
	                    4 as quantidadeDigitos,
	                    'PONTO' as separador,
	                    false as responsavelControleVagas,
	                    false as nivelSecretaria
	             union
	             select 4 as nivel,
	                    'Secretaria' as descricao,
	                    3 as quantidadeDigitos,
	                    'PONTO' as separador,
	                    true as responsavelControleVagas,
	                    true as nivelSecretaria
	             union                                     
					    select 5 as nivel,
	                    'Centro de Custo' as descricao,
	                    3 as quantidadeDigitos,
	                    'PONTO' as separador,
	                    false as responsavelControleVagas,
	                    false as nivelSecretaria
	                union     
						select 3 as nivel,
	                    'Unidade' as descricao,
	                    3 as quantidadeDigitos,
	                    'PONTO' as separador,
	                    false as responsavelControleVagas,
	                    false as nivelSecretaria
	             union                 
						select 2 as nivel,
	                    'Órgão' as descricao,
	                    2 as quantidadeDigitos,
	                    'PONTO' as separador,
	                    false as responsavelControleVagas,
	                    false as nivelSecretaria 
	   order by nivel) as suc) as niveis
	                    ) as a
	                    