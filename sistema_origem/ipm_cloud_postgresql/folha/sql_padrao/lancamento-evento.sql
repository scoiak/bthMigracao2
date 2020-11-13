select 
cpdv.cpdcodigo as configuracao,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', fcncodigo, funcontrato))) as matricula,
(case tipcodigo when 1 then 'MENSAL'when 2 then 'FERIAS'when 3 then 'RESCISAO'when 4 then 'DECIMO_TERCEIRO_SALARIO'when 5 then 'DECIMO_TERCEIRO_SALARIO'when 6 then 'DECIMO_TERCEIRO_SALARIO'when 7 then 'MENSAL'when 8 then 'MENSAL'when 9 then 'RESCISAO'when 10 then 'MENSAL'end) as tipoProcessamento,
(case tipcodigo when 1 then 'INTEGRAL'when 2 then 'INTEGRAL'when 3 then 'INTEGRAL'when 4 then 'INTEGRAL'when 5 then 'ADIANTAMENTO'when 6 then 'INTEGRAL'when 7 then 'INTEGRAL'when 8 then 'COMPLEMENTAR'when 9 then 'COMPLEMENTAR'when 10 then 'ADIANTAMENTO'end) as subTipoProcessamento,
(SELECT to_date(cpdv.odomesanovar ||'01','YYYYMMDD')::varchar) as dataInicial,
(SELECT to_date(cpdv.odomesanovar ||'01','YYYYMMDD')::varchar) as dataFinal,
clpvalor as valor,
null as observacao
from wfp.tbcalculoprovdescvar as cpdv join wfp.tbcalculoprovdesc as cpd on cpdv.calcodigo = cpd.calcodigo 
union 
select 
p.cpdcodigo as configuracao,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', fcncodigo, funcontrato))) as matricula,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
(SELECT to_date(p.parmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
(SELECT to_date(p.parmesanofinal ||'01','YYYYMMDD')::varchar) as dataFinal,
parvalor as valor,
null as observacao
from wfp.tbparcelamento as p 
where odomesano = 202010