select distinct
	 (CASE tipcodigo WHEN 1 then 'MENSAL' 	 WHEN 2 then 'FERIAS' 	 WHEN 3 then 'RESCISAO' 	 WHEN 4 then 'DECIMO_TERCEIRO_SALARIO' 	 WHEN 5 then 'DECIMO_TERCEIRO_SALARIO' 	 WHEN 6 then 'DECIMO_TERCEIRO_SALARIO' 	 WHEN 7 then 'MENSAL' 	 WHEN 8 then 'MENSAL' 	 WHEN 9 then 'RESCISAO' 	 WHEN 10 then 'MENSAL'  end) as tipoProcessamento,	 
	 (CASE tipcodigo WHEN 1 then 'INTEGRAL' 	 WHEN 2 then 'INTEGRAL' 	 WHEN 3 then 'INTEGRAL' 	 WHEN 4 then 'INTEGRAL' 	 WHEN 5 then 'ADIANTAMENTO' 	 WHEN 6 then 'INTEGRAL' 	 WHEN 7 then 'INTEGRAL' 	 WHEN 8 then 'COMPLEMENTAR' 	 WHEN 9 then 'COMPLEMENTAR' 	 WHEN 10 then 'ADIANTAMENTO'  end) as subTipoProcessamento,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,	 
	 substring(odomesano::varchar,1,4) || '-' || substring(odomesano::varchar,5,2) as competencia,
	 true as folhaPagamento,	 
	 (select sum(a.pagvalor) FROM wfp.tbpagamento as a,  wfp.tbprovDEsc
	  where  a.clicodigo =  tbprovDEsc.clicodigo
	   and a.odomesano =  tbprovDEsc.odomesano
	   and a.cpdcodigo =  tbprovDEsc.cpdcodigo and cpdclasse = 1
	   and  a.clicodigo =  tbpagamento.clicodigo
	   and a.odomesano =  tbpagamento.odomesano
	   and a.funcontrato =  tbpagamento.funcontrato
	   and a.fcncodigo =  tbpagamento.fcncodigo) as  totalBruto,
	 (select sum(a.pagvalor) FROM wfp.tbpagamento as a,  wfp.tbprovDEsc
	  where  a.clicodigo =  tbprovDEsc.clicodigo
	   and a.odomesano =  tbprovDEsc.odomesano
	   and a.cpdcodigo =  tbprovDEsc.cpdcodigo and cpdclasse = 2
	   and  a.clicodigo =  tbpagamento.clicodigo
	   and a.odomesano =  tbpagamento.odomesano
	   and a.funcontrato =  tbpagamento.funcontrato
	   and a.fcncodigo =  tbpagamento.fcncodigo) as totalDesconto,
	 (select sum(a.pagvalor) FROM wfp.tbpagamento as a,  wfp.tbprovDEsc
	  where  a.clicodigo =  tbprovDEsc.clicodigo
	   and a.odomesano =  tbprovDEsc.odomesano
	   and a.cpdcodigo =  tbprovDEsc.cpdcodigo and cpdclasse = 1
	   and  a.clicodigo =  tbpagamento.clicodigo
	   and a.odomesano =  tbpagamento.odomesano
	   and a.funcontrato =  tbpagamento.funcontrato
	   and a.fcncodigo =  tbpagamento.fcncodigo)- 
	   (select sum(a.pagvalor) FROM wfp.tbpagamento as a,  wfp.tbprovDEsc
	  where  a.clicodigo =  tbprovDEsc.clicodigo
	   and a.odomesano =  tbprovDEsc.odomesano
	   and a.cpdcodigo =  tbprovDEsc.cpdcodigo and cpdclasse = 2
	   and  a.clicodigo =  tbpagamento.clicodigo
	   and a.odomesano =  tbpagamento.odomesano
	   and a.funcontrato =  tbpagamento.funcontrato
	   and a.fcncodigo =  tbpagamento.fcncodigo) as totalLiquido,
	 (  select ododatafinal from wfp.tbperiodofolha where tbperiodofolha.clicodigo = tbpagamento.clicodigo and tbperiodofolha.odomesano = tbpagamento.odomesano) as dataFechamento,  
	 pagdata as dataPagamento,
	 null as dataLiberacao,
      pagdata as  dataCalculo,
      'FECHADA' as situacao,
	  --true as conversao, 
	  --------Dados Eventos
	  null as id_eventos, --buscar o id q subiu no Cloud
	  null as id_configuracao, --buscar o id q subiu no Cloud
	  cpdcodigo as evento_IPM,  --codigo do evento no IPM
	   (select (case cpdclasse when 1 then 'VENCIMENTO' when 2 then 'DESCONTO' when 3 then 'INFORMATIVO_MAIS' when 4 then 'INFORMATIVO_MENOS' end) 
		FROM  wfp.tbprovDEsc
	  where tbpagamento.clicodigo =  tbprovDEsc.clicodigo
	   and tbpagamento.odomesano =  tbprovDEsc.odomesano
	   and tbpagamento.cpdcodigo =  tbprovDEsc.cpdcodigo ) as tipo,
	  pagreferencia as  referencia,
	  pagvalor as valor,
     'NENHUMA'as classificacaoEvento,
	  'false' as lancamentoVariavel	   
	 FROM wfp.tbpagamento  
	 where odomesano = 202010  and fcncodigo = 4714

	 	select distinct 
	---composicaoBases
	null as id_configuracaoEvento, --buscar no Cloud
	null as id_base, --buscar no Cloud
 	(CASE cpdcodigo WHEN 36 then 'FGTS'
	 WHEN 50 then 'INSS'
     WHEN 51 then 'INSS13' 
	 WHEN 52 then 'PREVEST' 
	 WHEN 53 then 'PREVEST13'
	 WHEN 56 then 'FUNDOPREV' 
	 WHEN 57 then 'FUNDPREV13' 
	 WHEN 58 then 'IRRF' 
	 WHEN 59 then  'IRRF13'
	 WHEN 91 then 'FUPRFEPR'
	 WHEN 92 then 'IRRFFER' 
	 WHEN 116 then 'IRRFFERRESC' 
	 WHEN 126 then 'SALAFAM'  
	 WHEN 127  then 'SALAFAM'  ---- 'SALAFAMILUA Estatuatario'----Não tem similar no padrão betha Cloud ver se será necessário criar ou deixa como 'SALAFAM'
	-- WHEN 401 	"ABATIMENTO PREVBIGUAÇU(Cargo comissionado)" ----Não tem similar no padrão betha Cloud
	 WHEN 525 then 'INSS' 
	 WHEN 526 then 'INSS13'  
	 WHEN 660 then  'FUNDOPREV' end) as Sigla_Base,
	 pbavalor as valor
	
	  FROM wfp.tbpagamentobase   
	  --where Sigla_Base is not null 
	  --and odomesano = '202009'  and fcncodigo = 15185
	 
--SELECT * FROM wfp.tbcalculofolha;
--select * from wfp.tbrescisaocalculada;
