
select * from wfp.tbrubricaautonomoplano
select * from wfp.tbrubricaautonomo
select * from wfp.tbgpsgcencusautonomo

select * from public.controle_migracao_lotes where tipo_registro = 'organograma';
select * from public.controle_migracao_registro where tipo_registro = 'vinculo-empregaticio';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'vinculo-empregaticio';

select * from public.controle_migracao_registro where tipo_registro = 'matricula';

select * from wfp.tbfuncontrato where regcodigo in (15) and odomesano = 202009 and fcncodigo = 9236

SELECT id_lote, url_consulta FROM public.controle_migracao_lotes WHERE status not in (3, 4, 5) AND tipo_registro = 'pessoa-fisica'

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