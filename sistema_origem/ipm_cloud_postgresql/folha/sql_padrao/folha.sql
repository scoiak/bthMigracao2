create index IF NOT exists idx_p_p on wfp.tbpagamento (fcncodigo, funcontrato, odomesano, tipcodigo);
create index IF NOT exists idx_p_pb on wfp.tbpagamentobase (fcncodigo, funcontrato, odomesano);
create index IF NOT exists idx_p_pd on wfp.tbprovdesc (odomesano,cpdcodigo, cpdclasse);
create index IF NOT exists idx_p_irrf on wfp.tbirrf (odomesano,irrmesano);
create index IF NOT exists idx_p_pa on wfp.tbpensaoalimenticia (fcncodigo,funcontrato,odomesano);


select * from ( select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc, dataPagamento asc) as codigo,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', tipoCalculo, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), matricula, tipoProcessamento, subTipoProcessamento, dataPagamento))) as calculo,
* from (
select distinct
	(CASE tipcodigo WHEN 1 then 'calculo-folha-mensal' 	 WHEN 2 then 'calculo-folha-ferias' 	 WHEN 3 then 'calculo-folha-rescisao' 	 WHEN 4 then 'calculo-folha-decimo-terceiro' 	 WHEN 5 then 'calculo-folha-decimo-terceiro' 	 WHEN 6 then 'calculo-folha-decimo-terceiro' 	 WHEN 7 then 'calculo-folha-mensal' 	 WHEN 8 then 'calculo-folha-mensal' 	 WHEN 9 then 'calculo-folha-rescisao' 	 WHEN 10 then 'calculo-folha-mensal'  end) as tipoCalculo,	 
	 (CASE tipcodigo WHEN 1 then 'MENSAL' 	 WHEN 2 then 'FERIAS' 	 WHEN 3 then 'RESCISAO' 	 WHEN 4 then 'DECIMO_TERCEIRO_SALARIO' 	 WHEN 5 then 'DECIMO_TERCEIRO_SALARIO' 	 WHEN 6 then 'DECIMO_TERCEIRO_SALARIO' 	 WHEN 7 then 'MENSAL' 	 WHEN 8 then 'MENSAL' 	 WHEN 9 then 'RESCISAO' 	 WHEN 10 then 'MENSAL'  end) as tipoProcessamento,	 
	 (CASE tipcodigo WHEN 1 then 'INTEGRAL' 	 WHEN 2 then 'INTEGRAL' 	 WHEN 3 then 'INTEGRAL' 	 WHEN 4 then 'INTEGRAL' 	 WHEN 5 then 'ADIANTAMENTO' 	 WHEN 6 then 'INTEGRAL' 	 WHEN 7 then 'INTEGRAL' 	 WHEN 8 then 'COMPLEMENTAR' 	 WHEN 9 then 'COMPLEMENTAR' 	 WHEN 10 then 'ADIANTAMENTO'  end) as subTipoProcessamento,	 	 
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	 substring(odomesano::varchar,1,4) || '-' || substring(odomesano::varchar,5,2) as competencia,
	 true as folhaPagamento,	 
	 coalesce((select sum(a.pagvalor) FROM wfp.tbpagamento as a,  wfp.tbprovdesc as b
	  where a.odomesano =  b.odomesano
	   and a.cpdcodigo =  b.cpdcodigo and cpdclasse = 1	   
	   and a.odomesano =  p.odomesano
	   and a.funcontrato =  p.funcontrato
	   and a.fcncodigo =  p.fcncodigo 
	   and a.tipcodigo = p.tipcodigo),0) as  totalBruto,
	 coalesce((select sum(a.pagvalor) FROM wfp.tbpagamento as a,   wfp.tbprovdesc as b
	  where a.odomesano =  b.odomesano
	   and a.cpdcodigo =  b.cpdcodigo and cpdclasse = 2	   
	   and a.odomesano =  p.odomesano
	   and a.funcontrato =  p.funcontrato
	   and a.fcncodigo =  p.fcncodigo
	   and a.tipcodigo = p.tipcodigo),0) as totalDesconto,
	 coalesce((select sum(a.pagvalor) FROM wfp.tbpagamento as a,   wfp.tbprovdesc as b
	  where  a.odomesano =  b.odomesano
	   and a.cpdcodigo =  b.cpdcodigo and cpdclasse = 1	  
	   and a.odomesano =  p.odomesano
	   and a.funcontrato =  p.funcontrato
	   and a.fcncodigo =  p.fcncodigo
	   and a.tipcodigo = p.tipcodigo),0)- 
	   coalesce((select sum(a.pagvalor) FROM wfp.tbpagamento as a, wfp.tbprovdesc as b
	  where  a.odomesano =  b.odomesano
	   and a.cpdcodigo =  b.cpdcodigo and cpdclasse = 2	   
	   and a.odomesano =  p.odomesano
	   and a.funcontrato =  p.funcontrato
	   and a.fcncodigo =  p.fcncodigo
	   and a.tipcodigo = p.tipcodigo),0) as totalLiquido,
	 (select ododatafinal from wfp.tbperiodofolha where tbperiodofolha.clicodigo = p.clicodigo and tbperiodofolha.odomesano = p.odomesano)::varchar as dataFechamento,  
	 pagdata::varchar as dataPagamento,
	 null as dataLiberacao,
      pagdata::varchar as  dataCalculo,
	--substring(odomesano::varchar,1,4) || '-' || substring(odomesano::varchar,5,2) || '-' || '01' as dataFechamento,
      'FECHADA' as situacao,
	  --true as conversao, 	  
	  	(select string_agg(suc.configuracao || '%|%' || suc.tipo::varchar || '%|%' || coalesce(suc.referencia,0) || '%|%' || coalesce(suc.valor,0) || '%|%' || coalesce(suc.periodosAquisitivosFerias,'') || '%|%' || coalesce(suc.rateioDependentes,''),'%||%') 
	  	from ( select sum(suc.valor) as valor,sum(suc.referencia) as referencia,suc.tipo,suc.configuracao,suc.periodosAquisitivosFerias,suc.rateioDependentes from (	 select 	
	  	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-evento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), 		  	 
	  	(case suc.cpdcodigo 
	  	when 675 then 44 
	  	when 525 then 50 
	  	when 526 then 51
	  	else suc.cpdcodigo 
	  	end )	  		  		  	
	  	))) as configuracao, (select (case sucpd.cpdclasse when 1 then 'VENCIMENTO' when 2 then 'DESCONTO' when 3 then 'INFORMATIVO_MAIS' when 4 then 'INFORMATIVO_MENOS' end)		FROM  wfp.tbprovdesc as sucpd	  where suc.clicodigo =  sucpd.clicodigo	   and suc.odomesano =  sucpd.odomesano	   and suc.cpdcodigo =  sucpd.cpdcodigo )  as tipo,	  pagreferencia as  referencia,	  pagvalor as valor,
	  	null as periodosAquisitivosFerias,
	  	(case when suc.cpdcodigo in (44,675) then (
	  	/**/
	  	select string_agg(coalesce(xuc.dependencia::varchar,'') || '%&%' || coalesce(xuc.valor::varchar,''),'%&&%') from ( select (select id_gerado
	 	from public.controle_migracao_registro
	 	where hash_chave_dsk = md5(concat('300','dependencia', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = pa.unicodigodep),
	 									 (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = (select fc.unicodigo from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = 202010)),
	 									   pa.pnsdatainicio::varchar
	 									  ))) as dependencia,
	 									  pa.pnsreferencia as valor
	 									  from wfp.tbpensaoalimenticia as pa
	 									  where pa.fcncodigo = p.fcncodigo and pa.funcontrato = p.funcontrato and odomesano = 202011
	  	/**/
	  	) as xuc where dependencia is not null) else null end) as rateioDependentes
	  	FROM wfp.tbpagamento as suc	 where suc.fcncodigo = p.fcncodigo and  suc.funcontrato = p.funcontrato and suc.odomesano = p.odomesano and suc.tipcodigo = p.tipcodigo
	  	) as suc group by configuracao,tipo,periodosAquisitivosFerias,rateioDependentes
	  	union all
	  	select	
	  	((select count(d.unicodigodep) from wun.tbdependente as d where d.unicodigores = (select fc.unicodigo from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = 202010) and depir in (1,2)) * (select irrf.irrdeducaodep from wfp.tbirrf as irrf where irrf.odomesano = 202010 and (irrf.irrmesano = p.odomesano or irrf.irrmesano < p.odomesano) order by irrf.irrmesano desc limit 1)) as valor,
	  	(select count(d.unicodigodep) from wun.tbdependente as d where d.unicodigores = (select fc.unicodigo from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = 202010) and depir in (1,2)) as referencia,	 
	  	'INFORMATIVO_MENOS' as tipo,	 
	  	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-evento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),138))) as configuracao,
	  	null as periodosAquisitivosFerias,
	  	null as rateioDependentes
	    where (select count(d.unicodigodep) from wun.tbdependente as d where d.unicodigores = (select fc.unicodigo from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = 202010) and d.depir in (1,2)) > 0
	  	) as suc
	  	) as eventos,	  		  
		(select string_agg(suc.configuracao || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'base', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),suc.base))) || '%|%' || coalesce(suc.valor,0),'%||%') from (		 	select distinct 			 	(CASE pb.cpdcodigo  	WHEN 36 then 'FGTS'	 WHEN 50 then 'INSS'     WHEN 51 then 'INSS13' 	 WHEN 52 then 'PREVEST' 	 WHEN 53 then 'PREVEST13'	 WHEN 56 then 'FUNDOPREV' 	 WHEN 57 then 'FUNDPREV13' 	 WHEN 58 then 'IRRF' 	 WHEN 59 then  'IRRF13'	 WHEN 91 then 'FUPRFEPR'	 WHEN 92 then 'IRRFFER' 	 WHEN 116 then 'IRRFFERRESC' 	 WHEN 126 then 'SALAFAM'  	 WHEN 127  then 'SALAFAM'  /* 'SALAFAMILUA Estatuatario' Não tem similar no padrão betha Cloud ver se será necessário criar ou deixa como 'SALAFAM' WHEN 401 	"ABATIMENTO PREVBIGUAÇU(Cargo comissionado)" Não tem similar no padrão betha Cloud*/	 WHEN 525 then 'INSS' 	 WHEN 526 then 'INSS13'  	 WHEN 660 then  'FUNDOPREV' end) as base,	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-evento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), pb.cpdcodigo))) as configuracao,	 pb.pbavalor as valor		  FROM wfp.tbpagamentobase as pb	where pb.fcncodigo = p.fcncodigo and  pb.funcontrato = p.funcontrato and pb.odomesano = p.odomesano) as suc) as composicaoBases
	   FROM wfp.tbpagamento  as p	 
	 --where odomesano = 202010
where odomesano >= 202001	 
--and fcncodigo in (56, 2 ,7959, 10438, 4714)
) as a
) as b
where matricula is not null
and calculo is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'folha',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,tipoprocessamento,subtipoprocessamento,competencia,dataPagamento))) is null