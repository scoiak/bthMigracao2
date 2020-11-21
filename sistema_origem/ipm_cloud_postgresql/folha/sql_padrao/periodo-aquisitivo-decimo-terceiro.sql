create index IF NOT exists idx_contrato_codAno_admissao on wfp.tbfuncontrato (fcncodigo, funcontrato,fundataadmissao); 
create index IF NOT exists idx_rescisao_porfunc on wfp.tbrescisaocalculada (fcncodigo, funcontrato,resdatarescisao);

select distinct 
	   300 as sistema,
	   'periodo-aquisitivo-decimo-terceiro' as tipo_registro, 
       tbfuncontrato.fcncodigo as chave_dsk1,
       tbfuncontrato.funcontrato as chave_dsk2,
	   left(tbfuncontrato.odomesano::varchar,4)::int as chave_dsk3 , 
	   -- bethadba.dbf_get_id_gerado(sistema , tipo_registro , chave_dsk1 , chave_dsk2 , chave_dsk3 ) as id,
       -- bethadba.dbf_get_id_gerado( sistema , 'matricula' , chave_dsk1 , chave_dsk2) as matricula,
	   --date_part('year',tbfuncontrato.fundataadmissao) 
	   (left(tbfuncontrato.odomesano::varchar,4)::int) as anoExercicio,
	   (case when date_part('year',tbrescisaocalculada.resdatarescisao)::int != left(tbfuncontrato.odomesano::varchar,4)::int then null else  tbrescisaocalculada.resdatarescisao end) as dataRescisao,
	   (case when (coalesce(length((case when date_part('year',tbrescisaocalculada.resdatarescisao)::int != left(tbfuncontrato.odomesano::varchar,4)::int then 
	   			'-' 
	   		   else  tbrescisaocalculada.resdatarescisao::varchar end)),'1')::int) > 1 then 'S' else 'N' end) as temRescisao, 
	   'JANEIRO_DEZEMBRO' as configuracao,
	   (CASE WHEN 
            	tbfuncontrato.fundataadmissao > to_date((left(tbfuncontrato.odomesano::varchar,4)::int)::varchar||'/01/01','YYYY/MM/DD') 
       	   	THEN tbfuncontrato.fundataadmissao
	   ELSE to_date((left(tbfuncontrato.odomesano::varchar,4)::int)::varchar||'/01/01','YYYY/MM/DD') 
       end) as dataInicial,
       coalesce((case when date_part('year',tbrescisaocalculada.resdatarescisao)::int != left(tbfuncontrato.odomesano::varchar,4)::int then null else  tbrescisaocalculada.resdatarescisao end),to_date((left(tbfuncontrato.odomesano::varchar,4)::int)::varchar||'/12/31','YYYY/MM/DD')) as dataFinal,
       (case when (left(tbfuncontrato.odomesano::varchar,4)::int) = 2020 then 'EM_ANDAMENTO ' else 'QUITADO' end ) as situacao,
       12 as avosSemDescFaltasAfast, 
       12 as avosAdquiridosFgts, 
       12 as avosAdquiridos,
       0 as avosPerdidos,
       (199408) as menorAnoCalculo
  from wfp.tbfuncontrato left outer join  wfp.tbrescisaocalculada on (tbfuncontrato.fcncodigo = tbrescisaocalculada.fcncodigo
  																      and  tbfuncontrato.clicodigo = tbrescisaocalculada.clicodigo
  																      and  tbfuncontrato.funcontrato = tbrescisaocalculada.funcontrato)
 where left(tbfuncontrato.odomesano::varchar,4)::int between date_part('year',tbfuncontrato.fundataadmissao)::int 
 														and coalesce (date_part('year',tbrescisaocalculada.resdatarescisao)::int,2020)
  and  tbfuncontrato.fcncodigo IN(7479)
 order by tbfuncontrato.fcncodigo,
 		  tbfuncontrato.funcontrato,
		  left(tbfuncontrato.odomesano::varchar,4)::int
--limit 100