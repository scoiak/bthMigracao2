SELECT  	
	(CASE tipcodigo WHEN 1 then 'MENSAL' 
	 WHEN 2 then 'FERIAS' 
	 WHEN 3 then 'RESCISAO' 
	 WHEN 4 then 'DECIMO_TERCEIRO_SALARIO' 
	 WHEN 5 then 'DECIMO_TERCEIRO_SALARIO' 
	 WHEN 6 then 'DECIMO_TERCEIRO_SALARIO' 
	 WHEN 7 then 'MENSAL' 
	 WHEN 8 then 'MENSAL' 
	 WHEN 9 then 'RESCISAO' 
	 WHEN 10 then 'MENSAL'  end) as tipoProcessamento,	 
	 (CASE tipcodigo WHEN 1 then 'INTEGRAL' 
	 WHEN 2 then 'INTEGRAL' 
	 WHEN 3 then 'INTEGRAL' 
	 WHEN 4 then 'INTEGRAL' 
	 WHEN 5 then 'ADIANTAMENTO' 
	 WHEN 6 then 'INTEGRAL' 
	 WHEN 7 then 'INTEGRAL' 
	 WHEN 8 then 'COMPLEMENTAR' 
	 WHEN 9 then 'COMPLEMENTAR' 
	 WHEN 10 then 'ADIANTAMENTO'  end) as subTipoProcessamento,
	 null as dataAgendamento,
	 pagData as dataPagamento,
	 'INDIVIDUAL' tipoVinculacaoMatricula,-- 
	 ---CalculoFolhaMatriculas
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', clicodigo, fcncodigo, funcontrato))) as matricula,	 
	     NULL AS saldoFgts,
    true AS fgtsMesAnterior,
    --TRUE AS conversao,
    resdataaviso AS avisoPrevio,

	where fcncodigo = 56;