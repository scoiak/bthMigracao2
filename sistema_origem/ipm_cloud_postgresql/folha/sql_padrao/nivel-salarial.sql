select
	*
from (
select
	nivcodigo as id,
	nivcodigo as codigo,
	clicodigo,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade,
	(nivcodigo || ' - ' || nivdescricao) as descricao,
	nivsalariobase as valor,
	cast(regexp_replace(nivhoramensal, '\:\d{2}$', '', 'gi') as integer) as cargaHoraria,
	false as coeficiente,
	coalesce((select nv.vigdatavigen::varchar || ' 03:00:00' from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano order by nv.vigdatavigen desc limit 1),(select CURRENT_DATE::varchar || ' 00:00:00')) as inicioVigencia,
	coalesce((select nv.vigdatavigen::varchar || ' 00:00:00' from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano order by nv.vigdatavigen asc limit 1),(select CURRENT_DATE::varchar || ' 00:00:00')) as dataHoraCriacao,
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = (select nv.txjcodigo from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano and nv.txjcodigo is not null order by nv.vigdatavigen asc limit 1)), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = (select nv.txjcodigo from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano and nv.txjcodigo is not null order by nv.vigdatavigen asc limit 1) limit 1))))::varchar as atoCriacao,
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = (select nv.txjcodigo from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano and nv.txjcodigo is not null order by nv.vigdatavigen desc limit 1)), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = (select nv.txjcodigo from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano and nv.txjcodigo is not null order by nv.vigdatavigen desc limit 1) limit 1))))::varchar as ultimoAto,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','plano-cargo-salario', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))), 1))) as planoCargoSalario,
	null as classesReferencias,
	null as motivoAlteracao,
	null as reajusteSalarial,
	(select string_agg(n.nivdescricao || ' - ' || suc.nivcodigo || '%|%' || suc.vigsalariobase || '%|%' || cast(regexp_replace(nivhoramensal, '\:\d{2}$', '', 'gi') as integer) || '%|%' || 'false' || '%|%' || suc.vigdatavigen::varchar || ' 01:00:00' || '%|%' || suc.vigdatavigen::varchar || ' 01:00:00' || '%|%' || '' || '%|%' || '' || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','plano-cargo-salario', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))), 1))) || '%|%' || '' || '%|%' || '' || '%|%' || '','%||%') from (select * from wfp.tbnivelvigen as nv where nv.nivcodigo = n.nivcodigo and nv.odomesano = n.odomesano order by nv.vigdatavigen asc) as suc) as historicos
from 
	wfp.tbnivel as n
where odomesano = '202010'
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', id_entidade, codigo))) is null