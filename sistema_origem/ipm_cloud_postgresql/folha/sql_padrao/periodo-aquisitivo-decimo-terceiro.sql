create index IF NOT exists idx_contrato_codAno_admissao on wfp.tbfuncontrato (fcncodigo, funcontrato, fundataadmissao);
create index IF NOT exists idx_rescisao_porfunc on wfp.tbrescisaocalculada (fcncodigo, funcontrato, resdatarescisao);
create index IF NOT exists idx_dc_p on wfp.tbpagamento (fcncodigo, funcontrato, odomesano, tipcodigo, cpdcodigo);

select
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'periodo-aquisitivo-decimo-terceiro',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,anoExercicio))) as id_gerado,
* from ( select
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc, dataInicial asc) as codigo,
 * from ( select
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
* from (
select distinct on (fcncodigo,funcontrato,decanopagamento)
  		dc.fcncodigo as fcncodigo,
       dc.funcontrato as funcontrato,
	   decanopagamento::int as anoExercicio,
	   'JANEIRO_DEZEMBRO' as configuracao,
       12 as avosAdquiridosFgts,
       12 as avosAdquiridos,
       --(select count(ass.odomesano) from wfp.tbfuncontrato as ass where ass.fcncodigo = dc.fcncodigo and ass.funcontrato = dc.funcontrato and to_date(ass.odomesano || '01','YYYYMMDD') between to_date(dc.decanopagamento || '0101','YYYYMMDD') and to_date(dc.decanopagamento || '1231','YYYYMMDD')) as avosAdquiridos,
       12 as avosDireito,
       0 as avosPerdidos,
       (to_date((left(dc.decanopagamento::varchar,4)::int)::varchar||'/01/01','YYYY/MM/DD'))::varchar as dataInicial,
       (to_date((left(dc.decanopagamento::varchar,4)::int)::varchar||'/12/31','YYYY/MM/DD'))::varchar as dataFinal,
       'QUITADO' as situacao,
       (select string_agg((case suc.tipo when 4 then 'GRATIFICACAO_NATALINA' when 5 then 'ADIANTAMENTO_DECIMO_TERCEIRO' when 6 then 'GRATIFICACAO_NATALINA' end) ||'%|%' || 'true' || '%|%' || substring(suc.competencia::varchar,1,4) || '-' || substring(suc.competencia::varchar,5,2) || '%|%'|| suc.total ,'%||%') from (select sum(suc.pagvalor) as total,suc.tipcodigo as tipo,suc.odomesano as competencia from wfp.tbpagamento as suc where suc.fcncodigo = dc.fcncodigo and suc.funcontrato = dc.funcontrato and (substring(suc.odomesano::varchar,1,4))::int = dc.decanopagamento::int  and suc.tipcodigo in (4,5,6) and suc.cpdcodigo in (select ax.cpdcodigo from wfp.tbprovdesc as ax where ax.cpdclasse = 1) group by tipcodigo,odomesano) as suc) as movimentacoes
from wfp.tbdecimocalculado as dc
where odomesano = 202011
--and dc.fcncodigo IN (56, 2 ,7959, 10438, 4714)
and decanopagamento < 2020
and  dc.fcncodigo IN (11935)
union all
select distinct on (fcncodigo,funcontrato,decanopagamento)
  		dc.fcncodigo as fcncodigo,
       dc.funcontrato as funcontrato,
	   decanopagamento::int as anoExercicio,
	   'JANEIRO_DEZEMBRO' as configuracao,
       --12 as avosAdquiridos,
       (select count(ass.odomesano) from wfp.tbfuncontrato as ass where ass.fcncodigo = dc.fcncodigo and ass.funcontrato = dc.funcontrato and to_date(ass.odomesano || '01','YYYYMMDD') between to_date(dc.decanopagamento || '0101','YYYYMMDD') and to_date(dc.decanopagamento || '1231','YYYYMMDD')) as avosAdquiridosFgts,
       (select count(ass.odomesano) from wfp.tbfuncontrato as ass where ass.fcncodigo = dc.fcncodigo and ass.funcontrato = dc.funcontrato and to_date(ass.odomesano || '01','YYYYMMDD') between to_date(dc.decanopagamento || '0101','YYYYMMDD') and to_date(dc.decanopagamento || '1231','YYYYMMDD')) as avosAdquiridos,
       12 as avosDireito,
       0 as avosPerdidos,
       (to_date((left(dc.decanopagamento::varchar,4)::int)::varchar||'/01/01','YYYY/MM/DD'))::varchar as dataInicial,
       (to_date((left(dc.decanopagamento::varchar,4)::int)::varchar||'/12/31','YYYY/MM/DD'))::varchar as dataFinal,
       (case (select max(tipcodigo) from wfp.tbpagamento as suc where suc.fcncodigo = dc.fcncodigo and suc.funcontrato = dc.funcontrato and (substring(suc.odomesano::varchar,1,4))::int = dc.decanopagamento::int and tipcodigo in (4,5,6) group by tipcodigo) when 6 then 'QUITADO' when 4 then 'QUITADO' when 5 then 'QUITADO_PARCIALMENTE' else 'EM_ANDAMENTO' end) as situacao,
       --'QUITADO' as situacao,
       (select string_agg((case suc.tipo when 4 then 'GRATIFICACAO_NATALINA' when 5 then 'ADIANTAMENTO_DECIMO_TERCEIRO' when 6 then 'GRATIFICACAO_NATALINA' end) ||'%|%' || 'true' || '%|%' || substring(suc.competencia::varchar,1,4) || '-' || substring(suc.competencia::varchar,5,2) || '%|%'|| suc.total ,'%||%') from (select sum(suc.pagvalor) as total,suc.tipcodigo as tipo,suc.odomesano as competencia from wfp.tbpagamento as suc where suc.fcncodigo = dc.fcncodigo and suc.funcontrato = dc.funcontrato and (substring(suc.odomesano::varchar,1,4))::int = dc.decanopagamento::int  and suc.tipcodigo in (4,5,6) and suc.cpdcodigo in (select ax.cpdcodigo from wfp.tbprovdesc as ax where ax.cpdclasse = 1) group by tipcodigo,odomesano) as suc) as movimentacoes
       -- null as movimentacoes
from wfp.tbdecimocalculado as dc
where odomesano = 202011
and decanopagamento >= 2020
and dc.fcncodigo IN (11935)
) as a
) as b
) as c
where matricula is not null
and anoexercicio >= 2018
--and matricula in (select id_gerado from controle_migracao_registro where tipo_registro = 'matricula' and i_chave_dsk2 in ('7959'))
and (select fc.funsituacao from wfp.tbfuncontrato as fc where fc.fcncodigo = fcncodigo and fc.funcontrato = funcontrato and fc.odomesano = odomesano limit 1) = 1
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'periodo-aquisitivo-decimo-terceiro',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,anoExercicio))) is null

