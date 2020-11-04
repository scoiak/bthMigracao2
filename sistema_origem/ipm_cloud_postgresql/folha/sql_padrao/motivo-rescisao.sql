select	
	*
from (
	select 
		motcodigo as id,
		motcodigo as codigo,		
		(motdescricao || ' - ' || motcodigo) as descricao,
		(CASE
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA' then 'APOSENTADORIA'			
			WHEN UPPER(motdescricao) ~ '[RESCISÃO|DEMISSÃO]' then (case when UPPER(motdescricao) ~ 'EMPREGADO$' then 'INICIATIVA_EMPREGADO' else 'INICIATIVA_EMPREGADOR' end)		 			 	
			WHEN UPPER(motdescricao) ~ '[MORTE|FALESCIMENTO|EXONERAÇÃO]' then 'DISSOLUCAO_CONTRATO_TRABALHO'
		 	else null
		END) as tipo,
		(CASE
		 	WHEN UPPER(motdescricao) ~ '^APOSENTADORIA$' then 'APOSENTADORIA_ESPECIAL'
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA ESPECIAL' then 'APOSENTADORIA_ESPECIAL'
		 	WHEN UPPER(motdescricao) ~ 'APOSENTADORIA POR INVALIDEZ' then 'APOSENTADORIA_ESPECIAL'
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA POR IDADE' then 'APOSENTADORIA_IDADE'
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA POR TEMPO' then 'APOSENTADORIA_TEMPO_SERVICO'
		 	WHEN UPPER(motdescricao) ~ 'COM JUSTA CAUSA EPREGADOR' then 'RESCISAO_COM_JUSTA_CAUSA_INICIATIVA_EMPREGADOR'
		 	WHEN UPPER(motdescricao) ~ 'SEM JUSTA CAUSA EMPREGADOR' then 'RESCISAO_SEM_JUSTA_CAUSA_INICIATIVA_EMPREGADOR'
			WHEN UPPER(motdescricao) ~ 'COM JUSTA CAUSA EMPREGADO$' then 'RESCISAO_INICIATIVA_EMPREGADO_394_483_CLT'
		 	WHEN UPPER(motdescricao) ~ 'SEM JUSTA CAUSA EMPREGADO$' then 'RESCISAO_SEM_JUSTA_CAUSA_INICIATIVA_DO_EMPREGADO'
		 	WHEN UPPER(motdescricao) ~ '[MORTE|FALESCIMENTO]' then 'FALECIMENTO_EMPREGADO_OUTROS_MOTIVOS'
		 ELSE null END
		) as classificacao,
		(CASE	WHEN UPPER(motdescricao) ~ 'APOSENTADORIA' then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-afastamento', '13'))) ELSE (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-afastamento', '64'))) end ) as tipoAfastamento
		-- (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-afastamento', CAST(motcodigo as text)))) as tipoAfastamento	
	from wfp.tbmotivoafasta
	where mottipo = 4
	and odomesano = 202009
	order by motdescricao
) tb
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao', CAST(codigo as text)))) is null
