select
	*
from (
	select
		motcodigo as id,
		motcodigo as codigo,			
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as entidade,
		concat(motcodigo, ' - ', motdescricao) as descricao,
		(CASE
			WHEN UPPER(motdescricao) ~ 'APOSENTADORIA' then 'APOSENTADORIA'
			WHEN replace(UPPER(motdescricao),'ã','Ã') ~ 'RESCISÃO' then 
			(case 
			WHEN UPPER(motdescricao) ~ 'EMPREGADO$' then 'INICIATIVA_EMPREGADO'
			else 'INICIATIVA_EMPREGADOR'
			 end)
		 	WHEN replace(UPPER(motdescricao),'ã','Ã') ~ 'DEMISSÃO' then 
		 	(case 
			WHEN UPPER(motdescricao) ~ 'EMPREGADO$' then 'INICIATIVA_EMPREGADO'
			else 'INICIATIVA_EMPREGADOR'
			 end)		 	
		 	ELSE 'DISSOLUCAO_CONTRATO_TRABALHO'
		END) as tipo,
		(case
			WHEN UPPER(motdescricao) ~ 'DEMISS' then 'DEMISSAO'
			WHEN UPPER(motdescricao) ~ 'RESCIS' then 'DEMISSAO'
			WHEN UPPER(motdescricao) ~ 'RMINO' then 'RESCISAO_POR_TERMINO_CONTRATO'
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
		(case WHEN UPPER(motdescricao) ~ 'APOSENTADORIA' then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-afastamento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))), '13'))) ELSE (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-afastamento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))), '64'))) end ) as tipoAfastamento
	from wfp.tbmotivoafasta
	where mottipo = 4
	and odomesano = 202012
	order by motdescricao
) tb
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao', entidade, codigo))) is null