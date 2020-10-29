select 
	*
from (
select	
	nivcodigo as id,	
	nivcodigo as codigo,
	-- (nivdescricao || ' - ' || row_number() over(partition by nivdescricao order by nivdescricao)) as descricao,
	(nivdescricao || ' - ' || nivcodigo) as descricao,
	nivsalariobase as valor,
	cast(regexp_replace(nivhoramensal, '\:\d{2}$', '', 'gi') as integer) as cargaHoraria,
	false as coeficiente, 
	(select nv.vigdatavigen::varchar || ' 00:00:00' from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano order by nv.vigdatavigen desc limit 1) as inicioVigencia,	
	(select nv.vigdatavigen::varchar || ' 00:00:00' from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano order by nv.vigdatavigen desc limit 1) as dataHoraCriacao, 
	null as atoCriacao, 
	null as ultimoAto,
	586 as planoCargoSalario,
	null as classesReferencias,
	null as motivoAlteracao,
	null as reajusteSalarial,
	(select string_agg(n.nivdescricao || ' - ' || suc.nivcodigo || '%|%' || suc.vigsalariobase || '%|%' || cast(regexp_replace(nivhoramensal, '\:\d{2}$', '', 'gi') as integer) || '%|%' || 'false' || '%|%' || suc.vigdatavigen::varchar || ' 01:00:00' || '%|%' || suc.vigdatavigen::varchar || ' 01:00:00' || '%|%' || 'null' || '%|%' || 'null' || '%|%' || '586' || '%|%' || 'null' || '%|%' || 'null' || '%|%' || 'null','%||%') from (select * from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano order by nv.vigdatavigen asc) as suc) as historicos
from 
	wfp.tbnivel as n
where odomesano = '202009'
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', codigo))) is null
