SELECT 
	'RESCISAO' AS tipoProcessamento,
	(case tipcodigo when 3 then 'INTEGRAL' when 9 then 'COMPLEMENTAR' else null end) AS subTipoProcessamento,
    NULL AS dataAgendamento,
    NULL AS dataPagamento,
	'INDIVIDUAL'  AS tipoVinculacaoMatricula,
    NULL AS calculoFolhaMatriculas,
    TRUE AS conversao,
    resdataaviso AS avisoPrevio,
	motcodigo AS motivoRescisao,
    resdata AS dataRescisao,
    NULL AS ato,
    false AS consideraAvosPerdidosDecimoTerceiro,
    FALSE AS descontarFaltasFerias,
    FALSE AS trabalhouDiaRescisao,
    FALSE as reporVaga       
    from wfp.tbrescisaocalculada

SELECT * FROM wfp.tbcalculofolha;
select * from wfp.tbrescisaocalculada;
