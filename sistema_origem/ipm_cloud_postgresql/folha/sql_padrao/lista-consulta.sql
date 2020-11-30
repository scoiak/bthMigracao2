select tipo_registro as registro,data_hora_env ,data_hora_ret,(data_hora_ret  - data_hora_env) as totalRetorno, id_lote as lote from public.controle_migracao_lotes where length(id_lote) > 1 order by data_hora_env desc;

select * from controle_migracao_lotes cml  order by data_hora_env desc;
select * from controle_migracao_registro cmr where hash_chave_dsk = '410277bfd49c9a7a341820fc0cb94082'
select * from wfp.tbrubricaautonomoplano
select * from wfp.tbrubricaautonomo
select * from wfp.tbgpsgcencusautonomo

select * from wfp.tbfuntransferencia where odomesano = 202010

select * from wfp.tbpensaoalimenticia where fcncodigo = 7944 and odomesano = 202010

select substring('202001',1,4) --aux.parmesanoinicio

to_date(coalesce((select ((case when substring(aux.parmesanoinicio,5,2) = '01' then (substring(aux.parmesanoinicio,1,4)::int - 1) || substring(aux.parmesanoinicio,5,2) else (aux.parmesanoinicio - 1) end)) || '01'

SELECT to_date(((case when substring('202001',5,2) = '01' then   (substring('202001',1,4)::int - 1) || substring('202001',5,2) else '0' end)::int - 1) ||'01','YYYYMMDD')::varchar as dataInicial

select data_hora_env ,data_hora_ret,(data_hora_ret  - data_hora_env) as totalRetorno, id_lote as lote from public.controle_migracao_lotes  order by data_hora_env desc;

select data_hora_env ,data_hora_ret,(data_hora_ret  - data_hora_env) as totalRetorno, id_lote as lote from public.controle_migracao_lotes where tipo_registro = 'matricula' order by data_hora_env desc;

select * from public.controle_migracao_registro where tipo_registro = 'lancamento-evento' and hash_chave_dsk  = 'cda0b61044d4902674da4698b83dc72d';
select * from public.controle_migracao_lotes where tipo_registro = 'lancamento-evento' and conteudo_json like '%2738db17a14e99863d1eaf128c7d0122%';

select * from public.controle_migracao_lotes where tipo_registro = 'matricula';
select * from public.controle_migracao_registro where tipo_registro = 'matricula';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'matricula';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'tipo-afastamento';
select * from public.controle_migracao_registro where tipo_registro = 'tipo-afastamento' and id_gerado = '7941';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'afastamento';
select * from public.controle_migracao_registro where tipo_registro = 'afastamento';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'lancamento-evento';
select * from public.controle_migracao_registro where tipo_registro = 'lancamento-evento';

select
(select max(p.pagvalor) from wfp.tbpagamento as p where p.fcncodigo = fg.fcncodigo and p.funcontrato = fg.funcontrato and p.tipcodigo = 2 and p.pagdata between fg.fgodatainicio and fg.fgodatafinal and p.odomesano = fg.odomesano),
* 
from wfp.tbferiasgozada as fg 
where fcncodigo = 4714 
and odomesano = 202010

select * from wfp.tbdecimocalculado t 
select * from wfp.tbpagamento wer

select * from public.controle_migracao_registro where tipo_registro = 'calculo-folha-ferias';
select * from public.controle_migracao_registro where tipo_registro = 'lancamento-evento';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'lancamento-evento';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'categoria-trabalhador';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'vinculo-empregaticio';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'calculo-folha-decimo-terceiro';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'calculo-folha-rescisao';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'base';

(select irrf.irrdeducaodep from wfp.tbirrf as irrf where irrf.odomesano = 202010 and (irrf.irrmesano = p.odomesano or irrf.irrmesano < p.odomesano) order by irrf.irrmesano desc limit 1)

select * from public.controle_migracao_registro_ocor where tipo_registro = 'dependencia';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'folha';
select * from public.controle_migracao_registro where tipo_registro = 'folha';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'periodo-aquisitivo-decimo-terceiro';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'periodo-aquisitivo-ferias';
select * from public.controle_migracao_registro where tipo_registro = 'periodo-aquisitivo-ferias';
select * from public.controle_migracao_registro where tipo_registro = 'periodo-aquisitivo-decimo-terceiro' and i_chave_dsk2 = '2042775';
select * from public.controle_migracao_registro where tipo_registro = 'rescisao';
select * from public.controle_migracao_registro where tipo_registro = 'rescisao' and i_chave_dsk2 ='2041812';

select * from public.controle_migracao_registro_ocor where tipo_registro = 'lancamento-evento';

select * from public.controle_migracao_registro where tipo_registro = 'matricula' and id_gerado = 2000180;

select * from public.controle_migracao_registro where tipo_registro = 'matricula' and id_gerado is not null;


select * from public.controle_migracao_registro_ocor where tipo_registro = 'afastamento';
select * from public.controle_migracao_registro where tipo_registro = 'afastamento' and id_gerado is null;
select * from public.controle_migracao_lotes where tipo_registro = 'afastamento';
select * from public.controle_migracao_registro where tipo_registro = 'tipo-afastamento' and id_gerado = 7675;

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