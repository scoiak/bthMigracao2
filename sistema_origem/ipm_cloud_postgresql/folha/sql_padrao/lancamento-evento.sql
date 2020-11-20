select * from ( select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-evento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), evento))) as configuracao,
row_number() over(partition by fcncodigo,funcontrato order by fcncodigo asc,funcontrato asc, dataInicial asc) as codigo,
* from (
select  
fcncodigo,
funcontrato,
cpdcodigo as evento,
(case tipcodigo when 1 then 'MENSAL'when 2 then 'FERIAS'when 3 then 'RESCISAO'when 4 then 'DECIMO_TERCEIRO_SALARIO'when 5 then 'DECIMO_TERCEIRO_SALARIO'when 6 then 'DECIMO_TERCEIRO_SALARIO'when 7 then 'MENSAL'when 8 then 'MENSAL'when 9 then 'RESCISAO'when 10 then 'MENSAL'end) as tipoProcessamento,
(case tipcodigo when 1 then 'INTEGRAL'when 2 then 'INTEGRAL'when 3 then 'INTEGRAL'when 4 then 'INTEGRAL'when 5 then 'ADIANTAMENTO'when 6 then 'INTEGRAL'when 7 then 'INTEGRAL'when 8 then 'COMPLEMENTAR'when 9 then 'COMPLEMENTAR'when 10 then 'ADIANTAMENTO'end) as subTipoProcessamento,
--'MENSAL' as tipoProcessamento,
--'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(varmesano ||'01','YYYYMMDD')::varchar) as dataInicial,
to_char(DATE (concat(varmesano, '01')), 'yyyy-MM-dd') as dataInicial,
to_char(DATE (concat(varmesano, '01')), 'yyyy-MM-dd') as dataFinal,
varvalor::varchar as valor,
null as observacao
from wfp.tbprovdescvaria as pdv --join wfp.tbcalculoprovdesc as cpd on pdv.calcodigo = cpd.calcodigo 
where 0 = 0
--and fcncodigo in (7961)--7316,9782,70
-- and varvalor > 0  
union ALL 
select 
fcncodigo,
funcontrato,
cpdcodigo as evento,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(p.parmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
to_char(DATE (concat(parmesanoinicio, '01')), 'yyyy-MM-dd') as dataInicial,
--to_date(coalesce((select (case when substring(aux.parmesanoinicio,4,2) - 1) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')::varchar as dataFinal,
to_date(coalesce((select ((case when substring(aux.parmesanoinicio::varchar,5,2) = '01' then concat(substring(aux.parmesanoinicio::varchar,1,4)::int - 1,substring(aux.parmesanoinicio::varchar,5,2)::varchar) else (aux.parmesanoinicio - 1)::varchar end)) || '01' from wfp.tbparcelamento as aux where p.fcncodigo = aux.fcncodigo and p.funcontrato = aux.funcontrato and aux.cpdcodigo = p.cpdcodigo and p.parmesanofinal > aux.parmesanoinicio and p.parmesanoinicio < aux.parmesanoinicio order by aux.parmesanoinicio asc limit 1),parmesanofinal || '01'),'YYYYMMDD')::varchar as dataFinal,
parvalor::varchar as valor,
null as observacao
from wfp.tbparcelamento as p
where odomesano = 202010
-- and parvalor > 0
--and fcncodigo in (7961)--7316,9782,70
union ALL 
select 
fcncodigo,
funcontrato,
cpdcodigo as evento,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(fixmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
to_char(DATE (concat(fixmesanoinicio, '01')), 'yyyy-MM-dd') as dataInicial,
to_char(DATE (concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01')), 'yyyy-MM-dd') as dataFinal,
fixvalor::varchar as valor,
null as observacao
from wfp.tbprovdescfixo as pdf --join wfp.tbcalculoprovdesc as cpd on pdf.calcodigo = cpd.calcodigo 
where odomesano = 202010
-- and fixvalor > 0
--and fcncodigo in (7961)--7316,9782,70
union ALL 
select 
fcncodigo,
funcontrato,
pdf.cpdcodigo as evento,
'DECIMO_TERCEIRO_SALARIO' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
--(SELECT to_date(fixmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
to_char(DATE (concat(fixmesanoinicio, '01')), 'yyyy-MM-dd') as dataInicial,
to_char(DATE (concat((case when fixmesanofinal::varchar = '210013' then '210012' else fixmesanofinal::varchar end), '01')), 'yyyy-MM-dd') as dataFinal,
fixvalor::varchar as valor,
null as observacao
from wfp.tbprovdescfixo as pdf join wfp.tbprovdesc as pd on pdf.cpdcodigo = pd.cpdcodigo  and pdf.odomesano = pd.odomesano
where pdf.odomesano = 202010
and pd.cpdclasse = 1
-- and fixvalor > 0
--and fcncodigo in (7961)--7316,9782,70
) as a
) as b
where matricula is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'lancamento-evento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,codigo))) is null
