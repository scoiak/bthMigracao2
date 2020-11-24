create index IF NOT exists idx_contrato_codAno_admissao on wfp.tbfuncontrato (fcncodigo, funcontrato, fundataadmissao); 
create index IF NOT exists idx_rescisao_porfunc on wfp.tbrescisaocalculada (fcncodigo, funcontrato, resdatarescisao);
create index IF NOT exists idx_dc_p on wfp.tbpagamento (fcncodigo, funcontrato, odomesano, tipcodigo, cpdcodigo);

/*
select * from ( select 
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc, datainicial asc) as codigo,
 * from ( select
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
* from (
select distinct 	   	   
       tbfuncontrato.fcncodigo as fcncodigo,
       tbfuncontrato.funcontrato as funcontrato,
	   left(tbfuncontrato.odomesano::varchar,4)::int as anoExercicio , 
	   --date_part('year',tbfuncontrato.fundataadmissao) 	   
	   (case when date_part('year',tbrescisaocalculada.resdatarescisao)::int != left(tbfuncontrato.odomesano::varchar,4)::int then null else  tbrescisaocalculada.resdatarescisao end) as dataRescisao,
	   (case when (coalesce(length((case when date_part('year',tbrescisaocalculada.resdatarescisao)::int != left(tbfuncontrato.odomesano::varchar,4)::int then 
	   			'-' 
	   		   else  tbrescisaocalculada.resdatarescisao::varchar end)),'1')::int) > 1 then 'S' else 'N' end) as temRescisao, 
	   'JANEIRO_DEZEMBRO' as configuracao,
	   (CASE WHEN 
            	tbfuncontrato.fundataadmissao > to_date((left(tbfuncontrato.odomesano::varchar,4)::int)::varchar||'/01/01','YYYY/MM/DD') 
       	   	THEN tbfuncontrato.fundataadmissao
	   ELSE to_date((left(tbfuncontrato.odomesano::varchar,4)::int)::varchar||'/01/01','YYYY/MM/DD') 
       end)::varchar as dataInicial,
       coalesce((case when date_part('year',tbrescisaocalculada.resdatarescisao)::int != left(tbfuncontrato.odomesano::varchar,4)::int then null else  tbrescisaocalculada.resdatarescisao end),to_date((left(tbfuncontrato.odomesano::varchar,4)::int)::varchar||'/12/31','YYYY/MM/DD'))::varchar as dataFinal,
       (case when (left(tbfuncontrato.odomesano::varchar,4)::int) = 2020 then 'QUITADO_PARCIALMENTE ' else 'QUITADO' end ) as situacao,
       12 as avosAdquiridos, 
       12 as avosAdquiridosFgts, 
       12 as avosDireito,
       0 as avosPerdidos
--      , (199408) as menorAnoCalculo
  from wfp.tbfuncontrato left outer join  wfp.tbrescisaocalculada on (tbfuncontrato.fcncodigo = tbrescisaocalculada.fcncodigo  																      
  																      and  tbfuncontrato.funcontrato = tbrescisaocalculada.funcontrato)
 where left(tbfuncontrato.odomesano::varchar,4)::int between date_part('year',tbfuncontrato.fundataadmissao)::int 
 														and coalesce (date_part('year',tbrescisaocalculada.resdatarescisao)::int,2020)
--and  tbfuncontrato.fcncodigo IN(126)
 order by tbfuncontrato.fcncodigo,
 		  tbfuncontrato.funcontrato,
		  left(tbfuncontrato.odomesano::varchar,4)::int
) as a
) as b
) as c
where matricula is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'periodo-aquisitivo-decimo-terceiro',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,datainicial))) is null
*/

select * from ( select 
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
where odomesano = 202010
and decanopagamento < 2020
--and  dc.fcncodigo IN (4714)
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
       (select string_agg((case suc.tipo when 4 then 'GRATIFICACAO_NATALINA' when 5 then 'ADIANTAMENTO_DECIMO_TERCEIRO' when 6 then 'GRATIFICACAO_NATALINA' end) ||'%|%' || 'true' || '%|%' || substring(suc.competencia::varchar,1,4) || '-' || substring(suc.competencia::varchar,5,2) || '%|%'|| suc.total ,'%||%') from (select sum(suc.pagvalor) as total,suc.tipcodigo as tipo,suc.odomesano as competencia from wfp.tbpagamento as suc where suc.fcncodigo = dc.fcncodigo and suc.funcontrato = dc.funcontrato and (substring(suc.odomesano::varchar,1,4))::int = dc.decanopagamento::int  and suc.tipcodigo in (4,5,6) and suc.cpdcodigo in (select ax.cpdcodigo from wfp.tbprovdesc as ax where ax.cpdclasse = 1) group by tipcodigo,odomesano) as suc) as movimentacoes
from wfp.tbdecimocalculado as dc 
where odomesano = 202010
and decanopagamento >= 2020
--and  dc.fcncodigo IN (4714)
) as a
) as b
) as c
where matricula is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'periodo-aquisitivo-decimo-terceiro',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,anoExercicio))) is null
