create index if not exists idx_fg_pf on wfp.tbferiasgozada  (ferdatainicio, odomesano, fcncodigo);

select 
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), 1))) as configuracaoFerias,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	(case ferpaga when 1 then 'EM_ANDAMENTO' when 2 then 'QUITADO' when 3 then 'CANCELADO' else null end) as situacao,
	pf.ferdatainicio as dataInicial,
	pf.ferdatafinal as dataFinal,
	(select fg.fgomesanopagto from wfp.tbferiasgozada as fg where fg.ferdatainicio = pf.ferdatainicio and fg.odomesano = pf.odomesano and fg.fcncodigo = pf.fcncodigo) as competenciaFechamentoProvisao,
	ferdiasperda as faltas,
	ferdiasdireito as diasAdquiridos,
	null as cancelados,
	ferperdeudireito as suspensos,
	(case ferpaga when 1 then ferdiasdireito when 2 then 0 when 3 then 0 else null end) as saldo,
	null as pagou1TercoIntegral,
	true as conversao,
	null as diasAnuladosRescisao,
	(select string_agg(suc.fgodatainicio || '%|%' || suc.fgodatafinal || '%|%' || 'null' || '%|%','%||%') from (select * from wfp.tbferiasgozada as fg where fg.ferdatainicio = pf.ferdatainicio and fg.odomesano = pf.odomesano and fg.fcncodigo = pf.fcncodigo) as suc) as movimentacoes
from 
	wfp.tbperiodoferia as pf
where 
	pf.odomesano = 202010
