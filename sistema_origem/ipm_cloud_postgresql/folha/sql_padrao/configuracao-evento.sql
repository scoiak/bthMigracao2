select
	cpdcodigo as id,
	cpdcodigo as codigo,
	clicodigo,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as id_entidade,
	--'2734' as id_entidade,
	cpddescricao as descricao,
	'1990-01' as inicioVigencia,
	(case cpdclasse when 1 then 'VENCIMENTO' when 2 then 'DESCONTO' when 3 then 'INFORMATIVO_MAIS' when 4 then 'INFORMATIVO_MENOS' end) as tipo,
	null as classificacao,
	null as naturezaRubrica,
	null as classificacaoBaixaProvisao,
	(case cpdclasse when 1 then 'HORAS' when 2 then 'PERCENTUAL' when 3 then 'VALOR ' end) as unidade,
	'false' as enviaTransparencia,
	null as codigoEsocial,
	null as ato,
	null as incideDsr,
	null as compoemHorasMes,
	null as observacao,
	null as desabilitado,
	null as formula,
	null as configuracaoProcessamentos
from
	wfp.tbprovdesc
where
	odomesano = '202009';

select
	cpdcodigo as id,
	cpdcodigo as codigo,
	cpddescricao as descricao,
	'1990-01' as inicioVigencia,
	(case cpdclasse when 1 then 'VENCIMENTO' when 2 then 'DESCONTO' when 3 then 'INFORMATIVO_MAIS' when 4 then 'INFORMATIVO_MENOS' end) as tipo,
	null as classificacao,
	narcodigo as naturezaRubrica,
	null as classificacaoBaixaProvisao,
	(case cpdtipo when 1 then 'HORAS' when 2 then 'PERCENTUAL' when 3 then 'VALOR ' end) as unidade,
	(case cpdportaltransparencia when 3 then 'true' else 'false' end) as enviaTransparencia,
	null as codigoEsocial,
	txjcodigo as ato,
	null as incideDsr,
	null as compoemHorasMes,
	cpdfundamentolegal as observacao,
	null as desabilitado,
	null as formula,
	null as configuracaoProcessamentos
from
	wfp.tbprovdesc
where
	odomesano = '202009';