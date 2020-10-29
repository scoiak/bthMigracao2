select 
	null as configuracaoFerias,
	fcncodigo as matricula,
	ferpaga as situacao,
	ferdatainicio as dataInicial,
	ferdatafinal as dataFinal,
	null as competenciaFechamentoProvisao,
	ferdiasperda as faltas,
	ferdiasdireito as diasAdquiridos,
	null as cancelados,
	ferperdeudireito as suspensos,
	null as saldo,
	null as pagou1TercoIntegral,
	null as conversao,
	null as diasAnuladosRescisao,
	null as movimentacoes
from 
	wfp.tbperiodoferia 
where 
	odomesano = '202009'

select * from wfp.tbperiodoferia where odomesano = '202009'