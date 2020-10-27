select
	'300' as sistema,
	'motivo-rescisao' as tipo_registro,
	motcodigo as chave_dsk1,
	*
from (
	select distinct
		1 as id,
		motcodigo,
		motdescricao as descricao,
		(CASE
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA' then 'APOSENTADORIA'
			WHEN UPPER(motdescricao) ~ 'RESCISÃO' then 'INICIATIVA_EMPREGADOR'
		 	WHEN UPPER(motdescricao) ~ 'DEMISSÃO' then 'INICIATIVA_EMPREGADOR'
		 	ELSE 'DISSOLUCAO_CONTRATO_TRABALHO' END
		) as tipo,
		(CASE
		 	WHEN UPPER(motdescricao) ~ '^APOSENTADORIA$' then 'APOSENTADORIA_ESPECIAL'
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA ESPECIAL' then 'APOSENTADORIA_ESPECIAL'
		 	WHEN UPPER(motdescricao) ~ 'APOSENTADORIA POR INVALIDEZ' then 'APOSENTADORIA_ESPECIAL'
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA POR IDADE' then 'APOSENTADORIA_IDADE'
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA POR TEMPO' then 'APOSENTADORIA_TEMPO_SERVICO'
		 	WHEN UPPER(motdescricao) ~ 'COM JUSTA CAUSA EPREGADOR' then 'RESCISAO_COM_JUSTA_CAUSA_INICIATIVA_EMPREGADOR'
		 	WHEN UPPER(motdescricao) ~ 'SEM JUSTA CAUSA EMPREGADOR' then 'RESCISAO_SEM_JUSTA_CAUSA_INICIATIVA_EMPREGADOR'
			WHEN UPPER(motdescricao) ~ 'COM JUSTA CAUSA EMPREGADO$' then 'RESCISAO_COM_JUSTA_CAUSA_INICIATIVA_DO_EMPREGADO'
		 	WHEN UPPER(motdescricao) ~ 'SEM JUSTA CAUSA EMPREGADO$' then 'RESCISAO_SEM_JUSTA_CAUSA_INICIATIVA_DO_EMPREGADO'
		 	WHEN UPPER(motdescricao) ~ '[MORTE|FALESCIMENTO]' then 'FALECIMENTO_EMPREGADO_OUTROS_MOTIVOS'
		 ELSE null END
		) as classificacao,
		(CASE
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA' then 6764
		 	ELSE 6820 END
		) as id_motivo_afastamento,
		--(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-afastamento', CAST(motcodigo as text)))) as id_motivo_afastamento,
		COALESCE((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao', CAST(padcodigo as text)))), 0) as situacao_registro
	from wfp.tbmotivoafasta
	where mottipo = 4
	and odomesano >= 202001
	and odomesano <= 202012
	order by motdescricao
) tb
where situacao_registro = 0;