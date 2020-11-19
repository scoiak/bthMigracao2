
select * from wfp.tbrubricaautonomoplano
select * from wfp.tbrubricaautonomo
select * from wfp.tbgpsgcencusautonomo

select * from wfp.tbfuntransferencia where odomesano = 202010

select * from wfp.tbpensaoalimenticia where fcncodigo = 7944 and odomesano = 202010

select (data_hora_ret  - data_hora_env) as totalRetorno, id_lote as lote from public.controle_migracao_lotes where tipo_registro = 'matricula';

select * from public.controle_migracao_lotes where tipo_registro = 'matricula';
select * from public.controle_migracao_registro where tipo_registro = 'matricula';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'matricula';

select * from public.controle_migracao_registro where tipo_registro = 'cargo' and id_gerado = 105221;

select * from public.controle_migracao_registro where tipo_registro = 'matricula' and i_chave_dsk2 = '187';

select * from wfp.funconta

select * from wfp.tbfunlocais where odomesano = 202010 and fcncodigo = 171

select count(*),tipo_registro,sistema from public.controle_migracao_registro group by tipo_registro,sistema order by 1,2

select * from public.controle_migracao_registro where tipo_registro = 'pessoa-contas';
select * from public.controle_migracao_registro where tipo_registro = 'conta-bancaria';
select * from public.controle_migracao_registro where tipo_registro = 'conta-bancaria' and i_chave_dsk1 = '84408847968';
 
select * from wfp.tbfuncontrato where regcodigo in (15) and odomesano = 202009 and fcncodigo = 9236

SELECT id_lote, url_consulta FROM public.controle_migracao_lotes WHERE status not in (3, 4, 5) AND tipo_registro = 'pessoa-fisica'
wfp.tbfuncontrato 
SELECT to_date('202009'||'01','YYYYMMDD')::varchar || '00:00:00'

select ((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'lotacao-fisica',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), right('000' || cast(fc.cltcodigo as text), 3)))) || '%|%' || 'true' || '%|%' || fc.data || '%|%' ||  '%|%' || ) as lotacoesFisicas

select distinct fcncodigo from wfp.tbfuncontrato where regcodigo = 25 and odomesano = 202010 and funsituacao in (1,2)

select  distinct on (b,c) a,b,c from (
select 202009 as a,'a' as b,'b' as c 
union 
select 202009 as a,'a' as b,'b' as C 
union
select 202010 as a,'a' as b,'b' as c  
union 
select 202010 as a,'b' as b,'a' as c
union
select 202011 as a,'a' as b,'b' as c  
) as a
--order by a asc
select 
		'1' as id,
		ano,
		ano::varchar as organograma,
		('Ano ' || ano::varchar) as descricao,
		1 as nivel,
		null as sigla,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','configuracao-organograma', 1))) as configuracao
		from generate_series(1990,2020) as ano
	union
	
	select count(*)	
	from wfp.tbfuncontrato as fc  join wfp.tbfuncionario as f on f.fcncodigo = fc.fcncodigo and f.odomesano = fc.odomesano
where fc.odomesano = 202010
and fc.funsituacao in (1,2)

select (case coalesce((select p.tpvcodigo from wfp.tbfunpreviden as p where p.funcontrato = 1 and p.fcncodigo = 7032 and p.odomesano = 200803 and p.fprprincipal = 1 limit 1),0) when 0 then true else false end)

select * from controle_migracao_registro cmr where tipo_registro = 'afastamento'

select	
	(case codigo when 1 then 'S' when 2 then 'N' else 'K' end) as teste
	from (select 1 as codigo) as a