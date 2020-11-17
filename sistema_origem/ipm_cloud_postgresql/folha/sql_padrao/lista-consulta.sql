
select * from wfp.tbrubricaautonomoplano
select * from wfp.tbrubricaautonomo
select * from wfp.tbgpsgcencusautonomo

select * from wfp.tbfuntransferencia

select * from public.controle_migracao_registro where tipo_registro = 'formacao';
select * from public.controle_migracao_lotes where tipo_registro = 'matricula';
select * from public.controle_migracao_registro where tipo_registro = 'cargo' and i_chave_dsk1 = '2734' and i_chave_dsk2 = '486';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'matricula';

select count(*),tipo_registro,sistema from public.controle_migracao_registro group by tipo_registro,sistema order by 1,2

select * from public.controle_migracao_registro where tipo_registro = 'pessoa-contas';
 
select * from wfp.tbfuncontrato where regcodigo in (15) and odomesano = 202009 and fcncodigo = 9236

SELECT id_lote, url_consulta FROM public.controle_migracao_lotes WHERE status not in (3, 4, 5) AND tipo_registro = 'pessoa-fisica'
wfp.tbfuncontrato 
SELECT to_date('202009'||'01','YYYYMMDD')::varchar || '00:00:00'

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