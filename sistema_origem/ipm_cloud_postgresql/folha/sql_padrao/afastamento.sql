select * from (select
row_number() over() as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
row_number() over(partition by matricula order by matricula asc,inicioAfastamento asc) as codigo,
* from (
SELECT
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	afadatainicio::varchar as inicioAfastamento,
	afadatafinal::varchar as fimAfastamento,
	afadatafinal::varchar as retornoTrabalho,
	afaafadias::varchar as quantidade,
	--afaafadias as quantidadeDias,
	'DIAS' as unidade,
	(case
	 when motcodigo in (63,60,56,2,55,1) then 'ACIDENTE_DOENCA'
	 when motcodigo in (17,59,62,30,29,26,22,21, 18,47,54,53,50,49,48,46,45,43,44, 58,16,10, 61, 47, 52, 51, 59) then 'LICENCA'
	 when motcodigo in (5,6,7,8,25,15,9,33,32,31, 13,34,35,36,37,38,39,40,41,42,14) then 'RESCISAO'
	 when motcodigo in (20,12,11) then 'CEDENCIA'
	 when motcodigo in (3,4) then 'FALTAS'
	 else 'LICENCA'
	 end
	) as decorrente,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-afastamento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), motcodigo)))::varchar as tipoAfastamento,
	--(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = fa.txjcodigo), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = fa.txjcodigo limit 1))))::varchar as ato,
	null as ato,
	left(afaobs, 150) as motivo,
	null as descontar,
	null as competenciaDesconto,
	null as abonar,
	null as competenciaAbono,
	null as afastamentoOrigem,
	null as pessoaJuridica,
	null as tipoOnus,
	null as atestados
FROM
	wfp.tbfunafastamento as fa
where odomesano = 202011
--and fcncodigo in (2)--,70,565
union all
select distinct on (matricula) * from (
SELECT distinct
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	pagdata::varchar as inicioAfastamento,
	null::varchar as fimAfastamento,
	null::varchar as retornoTrabalho,
	null::varchar as quantidade,
	--null as quantidadeDias,
	'DIAS' as unidade,
	'RESCISAO' as decorrente,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-afastamento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),64)))::varchar as tipoAfastamento,
		null as ato,
	null as motivo,
	null as descontar,
	null as competenciaDesconto,
	null as abonar,
	null as competenciaAbono,
	null as afastamentoOrigem,
	null as pessoaJuridica,
	null as tipoOnus,
	null as atestados
	FROM wfp.tbpagamento as p
	where tipcodigo in (3,9)
union all
select
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	rctdatarescisao::varchar as inicioAfastamento,
	null::varchar as fimAfastamento,
	null::varchar as retornoTrabalho,
	null::varchar as quantidade,
	--null as quantidadeDias,
	'DIAS' as unidade,
	'RESCISAO' as decorrente,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-afastamento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),64)))::varchar as tipoAfastamento,
	null as ato,
	null as motivo,
	null as descontar,
	null as competenciaDesconto,
	null as abonar,
	null as competenciaAbono,
	null as afastamentoOrigem,
	null as pessoaJuridica,
	null as tipoOnus,
	null as atestados
from  wfp.tbrescisaocontrato as r
where odomesano = 202011
and not exists (select fc.funsituacao from wfp.tbfuncontrato as fc where fc.funcontrato = r.funcontrato and fc.fcncodigo = r.fcncodigo and fc.odomesano = r.odomesano and fc.funsituacao = 1)
--and fcncodigo in (2)--,70,565
) as a
union all
select
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigosol, funcontratosol))) as matricula,
	ftrdatasolicitacao::varchar as inicioAfastamento,
	null::varchar as fimAfastamento,
	null::varchar as retornoTrabalho,
	null::varchar as quantidade,
	--null as quantidadeDias,
	'DIAS' as unidade,
	'CEDENCIA' as decorrente,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-afastamento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),20)))::varchar as tipoAfastamento,
	null as ato,
	null as motivo,
	null as descontar,
	null as competenciaDesconto,
	null as abonar,
	null as competenciaAbono,
	null as afastamentoOrigem,
	null as pessoaJuridica,
	null as tipoOnus,
	null as atestados
from wfp.tbfuntransferencia as ft
where odomesano = 202011
--and fcncodigo in (2)--,70,565
union all
select
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'matricula', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), fcncodigo, funcontrato))) as matricula,
	fgodatainicio::varchar as inicioAfastamento,
	fgodatafinal::varchar as fimAfastamento,
	fgodatafinal::varchar as retornoTrabalho,
	fgodiasgozo::varchar as quantidade,
	--null as quantidadeDias,
	'DIAS' as unidade,
	'FERIAS' as decorrente,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-afastamento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),65)))::varchar as tipoAfastamento,
	null as ato,
	null as motivo,
	null as descontar,
	null as competenciaDesconto,
	null as abonar,
	null as competenciaAbono,
	null as afastamentoOrigem,
	null as pessoaJuridica,
	null as tipoOnus,
	null as atestados
from wfp.tbferiasgozada as fg
where odomesano = 202011
--and fcncodigo in (3876)--,70,565,2
) as a
) as b
where matricula is not null
--and matricula in (2392849, 2392850, 2392851, 2392852)
--select * from controle_migracao_registro cmr where tipo_registro = 'afastamento' and i_chave_dsk2 = '2000180'
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'afastamento',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))),matricula,inicioafastamento))) is null