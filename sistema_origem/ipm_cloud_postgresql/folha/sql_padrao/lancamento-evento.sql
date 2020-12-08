create index IF NOT exists idx_fc_le on wfp.tbfuncontrato (fcncodigo, funcontrato, odomesano);

select * from ( select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-evento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), evento))),0) as configuracao,
row_number() over(partition by fcncodigo,funcontrato order by fcncodigo asc,funcontrato asc, dataInicial asc) as codigo,
* from (
select  
'VARIAVEL' as tipo,
fcncodigo,
funcontrato,
cpdcodigo as evento,
(case tipcodigo when 1 then 'MENSAL'when 2 then 'FERIAS'when 3 then 'RESCISAO'when 4 then 'DECIMO_TERCEIRO_SALARIO'when 5 then 'DECIMO_TERCEIRO_SALARIO'when 6 then 'DECIMO_TERCEIRO_SALARIO'when 7 then 'MENSAL'when 8 then 'MENSAL'when 9 then 'RESCISAO'when 10 then 'MENSAL'end) as tipoProcessamento,
(case tipcodigo when 1 then 'INTEGRAL'when 2 then 'INTEGRAL'when 3 then 'INTEGRAL'when 4 then 'INTEGRAL'when 5 then 'ADIANTAMENTO'when 6 then 'INTEGRAL'when 7 then 'INTEGRAL'when 8 then 'COMPLEMENTAR'when 9 then 'COMPLEMENTAR'when 10 then 'ADIANTAMENTO'end) as subTipoProcessamento,
--'MENSAL' as tipoProcessamento,
--'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(varmesano ||'01','YYYYMMDD')::varchar) as dataInicial,
--to_char(DATE (concat(varmesano, '01')), 'yyyy-MM-dd') as dataInicial,
to_char((case when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdv.fcncodigo and fc.funcontrato = pdv.funcontrato  order by fc.odomesano desc limit 1) < date(concat(varmesano, '01')) then date(concat(varmesano, '01')) else (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdv.fcncodigo and fc.funcontrato = pdv.funcontrato order by fc.odomesano desc limit 1) end), 'yyyy-MM-dd') as dataInicial,
to_char((case
when (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = pdv.fcncodigo and fc.funcontrato = pdv.funcontrato order by fc.odomesano desc limit 1) > date(concat(varmesano, '01')) 
then (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = pdv.fcncodigo and fc.funcontrato = pdv.funcontrato order by fc.odomesano desc limit 1)  
when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundatatermcont::date,('2021-01-01')::date) else ('3000-01-01')::date end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdv.fcncodigo and fc.funcontrato = pdv.funcontrato order by fc.odomesano desc limit 1) < date(concat(varmesano, '01')) 
then (select fc.fundatatermcont from wfp.tbfuncontrato as fc where fc.fcncodigo = pdv.fcncodigo and fc.funcontrato = pdv.funcontrato order by fc.odomesano desc limit 1)   
else date(concat(varmesano, '01')) 
end), 'yyyy-MM-dd') as dataFinal,
--varvalor::varchar as valor,
coalesce(to_char(varvalor, 'FM99999990.00'),'0.00') as valor,
null as observacao
from wfp.tbprovdescvaria as pdv --join wfp.tbcalculoprovdesc as cpd on pdv.calcodigo = cpd.calcodigo 
--where fcncodigo in (17534)--7316,9782,70,7961,14369,461,17586,4714,17586
-- and varvalor > 0  
union ALL 
select 
'PARCELAMENTO' as tipo,
fcncodigo,
funcontrato,
cpdcodigo as evento,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(p.parmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
--to_char(DATE (concat(parmesanoinicio, '01')), 'yyyy-MM-dd') as dataInicial,
to_char((case when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1) < date(concat(parmesanoinicio, '01')) then date(concat(parmesanoinicio, '01')) else (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1) end), 'yyyy-MM-dd') as dataInicial,
--to_date(coalesce((select (case when substring(aux.parmesanoinicio,4,2) - 1) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')::varchar as dataFinal,
--to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,'12'/*substring(aux.parmesanoinicio::varchar,5,2)::varchar*/) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')::varchar as dataFinal,
case
when (
to_char((case
when (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1) > to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,'12'/*substring(aux.parmesanoinicio::varchar,5,2)::varchar*/) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')
then (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1)
when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundatatermcont::date,('2021-01-01')::date) else ('3000-01-01')::date end)  from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1) < to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,'12'/*substring(aux.parmesanoinicio::varchar,5,2)::varchar*/) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')
then (select fc.fundatatermcont  from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1)
else to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,'12'/*substring(aux.parmesanoinicio::varchar,5,2)::varchar*/) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')
end), 'yyyy-MM-dd')::varchar)::date >= '2020-11-01' then '2020-11-01' else
to_char((case
when (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1) > to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,'12'/*substring(aux.parmesanoinicio::varchar,5,2)::varchar*/) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')
then (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1)
when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundatatermcont::date,('2021-01-01')::date) else ('3000-01-01')::date end)  from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1) < to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,'12'/*substring(aux.parmesanoinicio::varchar,5,2)::varchar*/) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')
then (select fc.fundatatermcont  from wfp.tbfuncontrato as fc where fc.fcncodigo = p.fcncodigo and fc.funcontrato = p.funcontrato and fc.odomesano = p.odomesano limit 1)
else to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,'12'/*substring(aux.parmesanoinicio::varchar,5,2)::varchar*/) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')
end), 'yyyy-MM-dd')::varchar end
as dataFinal,
--parvalor::varchar as valor,
coalesce(to_char(parvalor, 'FM99999990.00'),'0.00') as valor,
null as observacao
from wfp.tbparcelamento as p
where odomesano = 202011
-- and parvalor > 0
--and fcncodigo in (17586)--7316,9782,70,7961,14369,461,17586,4714
union ALL
select
'FIXO' as tipo,
fcncodigo,
funcontrato,
cpdcodigo as evento,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(fixmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
--to_char(DATE (concat(fixmesanoinicio, '01')), 'yyyy-MM-dd') as dataInicial,
to_char((case when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) < date(concat(fixmesanoinicio, '01')) then date(concat(fixmesanoinicio, '01')) else (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) end), 'yyyy-MM-dd') as dataInicial,
--to_char(date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01')), 'yyyy-MM-dd') as dataFinal,
to_char((case
when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundatatermcont::date,('2021-01-01')::date) else ('3000-01-01')::date end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) < date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01'))
then (select fundatatermcont from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1)
when (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) > date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01'))
then (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1)
else date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01'))
end),
'yyyy-MM-dd') as dataFinal,
--fixvalor::varchar as valor,
coalesce(to_char(fixvalor, 'FM99999990.00'),'0.00') as valor,
null as observacao
from wfp.tbprovdescfixo as pdf --join wfp.tbcalculoprovdesc as cpd on pdf.calcodigo = cpd.calcodigo
where odomesano = 202011
-- and fixvalor > 0
--and fcncodigo in (17586)--7316,9782,70,7961,14369,461,17586,4714
union ALL
select
'FIXO_DECIMO' as tipo,
fcncodigo,
funcontrato,
pdf.cpdcodigo as evento,
'DECIMO_TERCEIRO_SALARIO' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(fixmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
to_char((case when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) < date(concat(fixmesanoinicio, '01')) then date(concat(fixmesanoinicio, '01')) else (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundataadmissao::date,('2021-01-01')::date) else fc.fundataadmissao  end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) end), 'yyyy-MM-dd') as dataInicial,
--to_char(date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01')), 'yyyy-MM-dd') as dataFinal,
to_char((case
when (select (case when fc.funtipocontrato in (2) then coalesce(fc.fundatatermcont::date,('2021-01-01')::date) else ('3000-01-01')::date end) from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) < date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01'))
then (select fundatatermcont from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1)
when (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1) > date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01'))
then (select fc.fundataadmissao from wfp.tbfuncontrato as fc where fc.fcncodigo = pdf.fcncodigo and fc.funcontrato = pdf.funcontrato and fc.odomesano = pdf.odomesano limit 1)
else date(concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01'))
end),
'yyyy-MM-dd') as dataFinal,
--fixvalor::varchar as valor,
coalesce(to_char(fixvalor, 'FM99999990.00'),'0.00') as valor,
null as observacao
from wfp.tbprovdescfixo as pdf
join wfp.tbprovdesc as pd on pdf.cpdcodigo = pd.cpdcodigo  and pdf.odomesano = pd.odomesano
and exists (select 1 from wfp.tbparamprovdesc param where param.cpdcodigo = pdf.cpdcodigo and param.odomesano = pdf.odomesano and param.ppdbase13sdiferenca = 1 limit 1)
where pdf.odomesano = 202011
and pd.cpdclasse = 1
-- and fixvalor > 0
--and fcncodigo in (17586)--7316,9782,70,7961,14369,461,17586,4714
) as a
) as b
where matricula is not null
--and matricula in (select id_gerado from controle_migracao_registro where tipo_registro = 'matricula' and i_chave_dsk2 in ('238'))
--and dataInicial > dataFinal
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'lancamento-evento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,configuracao,tipoProcessamento,subTipoProcessamento,dataInicial,dataFinal))) is null
and coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-evento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), evento))),0) > 0
