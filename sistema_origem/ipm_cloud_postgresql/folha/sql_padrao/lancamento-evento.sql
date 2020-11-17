select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', fcncodigo, funcontrato))) as matricula,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-evento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), evento))) as configuracao,
row_number() over(partition by fcncodigo,funcontrato order by fcncodigo asc,funcontrato asc, dataInicial asc) as codigo,
* from (
select  
fcncodigo,
funcontrato,
cpdcodigo as evento,
--(case tipcodigo when 1 then 'MENSAL'when 2 then 'FERIAS'when 3 then 'RESCISAO'when 4 then 'DECIMO_TERCEIRO_SALARIO'when 5 then 'DECIMO_TERCEIRO_SALARIO'when 6 then 'DECIMO_TERCEIRO_SALARIO'when 7 then 'MENSAL'when 8 then 'MENSAL'when 9 then 'RESCISAO'when 10 then 'MENSAL'end) as tipoProcessamento,
--(case tipcodigo when 1 then 'INTEGRAL'when 2 then 'INTEGRAL'when 3 then 'INTEGRAL'when 4 then 'INTEGRAL'when 5 then 'ADIANTAMENTO'when 6 then 'INTEGRAL'when 7 then 'INTEGRAL'when 8 then 'COMPLEMENTAR'when 9 then 'COMPLEMENTAR'when 10 then 'ADIANTAMENTO'end) as subTipoProcessamento,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
(SELECT to_date(varmesano ||'01','YYYYMMDD')::varchar) as dataInicial,
(SELECT to_date(varmesano ||'01','YYYYMMDD')::varchar) as dataFinal,
varvalor as valor,
null as observacao
from wfp.tbprovdescvaria as pdv --join wfp.tbcalculoprovdesc as cpd on pdv.calcodigo = cpd.calcodigo 
where varvalor > 0
union ALL 
select 
fcncodigo,
funcontrato,
cpdcodigo as evento,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
(SELECT to_date(p.parmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
(SELECT to_date(p.parmesanofinal ||'01','YYYYMMDD')::varchar) as dataFinal,
parvalor as valor,
null as observacao
from wfp.tbparcelamento as p 
where odomesano = 202010
and parvalor > 0
union ALL 
select 
fcncodigo,
funcontrato,
cpdcodigo as evento,
'MENSAL' as tipoProcessamento,
'INTEGRAL' as subTipoProcessamento,
(SELECT to_date(fixmesanoinicio ||'01','YYYYMMDD')::varchar) as dataInicial,
(SELECT to_date(fixmesanofinal ||'01','YYYYMMDD')::varchar) as dataFinal,
fixvalor as valor,
null as observacao
from wfp.tbprovdescfixo as pdf --join wfp.tbcalculoprovdesc as cpd on pdf.calcodigo = cpd.calcodigo 
where fixvalor > 0
and odomesano = 202010
) as a
