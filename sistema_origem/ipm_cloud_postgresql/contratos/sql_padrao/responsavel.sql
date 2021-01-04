select
	row_number() over() as id,
	'305' as sistema,
	'responsavel' as tipo_registro,
	*
from (
	select
		p.*,
		2016 as clicodigo,
		true as eh_responsavel_municipio,
		null as natureza_cargo,
		p.unicodigo as matricula,
		null as natureza_cargo,
		true as ativo, 
		null as complemento_funcao,
		null as data_inativacao,
		(select r.mbcatribuicao from wco.tbintegrante r where r.unicodigo = p.unicodigo order by r.mbcdatainclusao desc limit 1) as mbcatribuicao,
		unaccent(upper(trim((select r.mbccargo from wco.tbintegrante r where r.unicodigo = p.unicodigo order by r.mbcdatainclusao desc limit 1)))) as cargo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))) as id_entidade,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'cargo', 
																									(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))), 
																									unaccent(upper(trim((select r.mbccargo from wco.tbintegrante r where r.unicodigo = p.unicodigo order by r.mbcdatainclusao desc limit 1))))))) as id_cargo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'responsavel', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))), p.cpf))) as id_gerado
	from (
		select distinct 
			p.unicodigo,
			(regexp_replace(p.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf,
			p.uninomerazao as nome
		from wco.tbintegrante r
		natural join wun.tbunico p
		order by 3
	) as p
) as tab 
where id_gerado is null
