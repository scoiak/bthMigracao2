-- A extensão abaixo permite a execução de uma função que formata a string removendo acentos das palavras
create extension if not exists unaccent;

select
	row_number() over() as id,
	'305' as sistema,
	'prazo-entrega' as tipo_registro,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))) as id_entidade,
	*
from (
	select distinct
	   clicodigo,
	   upper(unaccent(left(coalesce(trim(edtpreventrmat),'Imediata'), 50))) as descricao,
	   'UNICA'  tipo_execucao,
	   (case
	   		when coalesce(trim(edtpreventrmat), 'Imediata') ~ 'DIA' then 'DIAS'
	   		when coalesce(trim(edtpreventrmat), 'Imediata') ~ 'MES' then 'MESES'
	   		else 'OUTROS'
	   	end ) as unidade_entrega,
	   (case
	   	when coalesce(trim(edtpreventrmat), 'Imediata') ~ '[DIA|MES]' then coalesce((select unnest((REGEXP_MATCHES(left(coalesce(trim(edtpreventrmat), 'Imediata'), 50), '\d+', 'g'))) limit 1)::integer, 1)
	   	else 1
		end) as num_dias_meses,
	   (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(/* Sistema     */ 305,
	   																						      /* Tipo Reg.   */'prazo-entrega',
	   																						      /* Id Entidade */ (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))),
	   																						      /* Descrição   */ upper(unaccent(left(coalesce(trim(edtpreventrmat),'Imediata'), 50)))))) as id_gerado
	from wco.tbedital
	where edtpreventrmat is not null
	and edtpreventrmat != ''
	and clicodigo = {{clicodigo}}
) tab
where id_gerado is null
order by descricao