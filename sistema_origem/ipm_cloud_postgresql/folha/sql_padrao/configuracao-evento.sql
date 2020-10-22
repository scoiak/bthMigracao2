select
	cpdcodigo as id,
	cpdcodigo as codigo,
	cpddescricao as descricao,
	date('1990-01-01') as inicioVigencia,
	(case cpdclasse when 1 then 'PROVENTO' when 2 then 'DESCONTO' when 3 then 'INFORMATIVO_MAIS' when 4 then 'INFORMATIVO_MENOS' end) as tipo,
	null as classificacao,
	null as naturezaRubrica,
	null as classificacaoBaixaProvisao,
	(case cpdclasse when 1 then 'HORAS' when 2 then 'PERCENTUAL' when 3 then 'VALOR ' end) as unidade,
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