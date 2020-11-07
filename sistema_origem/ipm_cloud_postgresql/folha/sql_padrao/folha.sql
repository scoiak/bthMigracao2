select
row_number() over() as id,
row_number() over() as codigo,
null as calculo,
(case tipcodigo 
when 1 then 'MENSAL'
when 2 then 'FERIAS '
when 3 then 'RESCISAO'
when 4 then 'DECIMO_TERCEIRO_SALARIO'
when 5 then 'DECIMO_TERCEIRO_SALARIO'
when 6 then 'DECIMO_TERCEIRO_SALARIO'
when 7 then 'DECIMO_TERCEIRO_SALARIO'
when 8 then 'MENSAL'
when 9 then 'RESCISAO'
when 10 then 'MENSAL'
when 11 then 'MENSAL'
when 12 then 'MENSAL'
when 13 then 'MENSAL'
else null
end) as tipoProcessamento,
(case tipcodigo 
when 1 then 'INTEGRAL'
when 2 then 'INTEGRAL '
when 3 then 'INTEGRAL'
when 4 then 'INTEGRAL'
when 5 then 'COMPLEMENTAR '
when 6 then 'COMPLEMENTAR '
when 7 then 'COMPLEMENTAR'
when 8 then 'COMPLEMENTAR '
when 9 then 'COMPLEMENTAR '
when 10 then 'ADIANTAMENTO'
when 11 then 'INTEGRAL'
when 12 then 'INTEGRAL'
when 13 then 'INTEGRAL'
else null
end) as subTipoProcessamento,
fcncodigo as matricula,
resmesano as competencia,
true as folhaPagamento,
null as totalBruto,
null as totalDesconto,
null as totalLiquido,
null as dataFechamento,
null as dataPagamento,
null as dataLiberacao,
null as dataCalculo,
null as situacao,
true as conversao,
null as eventos,
null as composicaoBases,
null as movimentacoes
FROM wfp.tbrescisaocalculada

SELECT * FROM wfp.tbcalculofolha;
select * from wfp.tbrescisaocalculada;
select * from  wfp.calculoprovdesc
