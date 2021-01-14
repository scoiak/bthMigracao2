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
		true as ativo,
		null as complemento_funcao,
		null as data_inativacao,
		(select r.mbcatribuicao from wco.tbintegrante r where r.unicodigo = p.unicodigo order by r.mbcdatainclusao desc limit 1) as mbcatribuicao,
		unaccent(upper(trim((select r.mbccargo from wco.tbintegrante r where r.unicodigo = p.unicodigo order by r.mbcdatainclusao desc limit 1)))) as cargo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))) as id_entidade,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'cargo',
																									(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))),
																									unaccent(upper(trim((select r.mbccargo from wco.tbintegrante r where r.unicodigo = p.unicodigo order by r.mbcdatainclusao desc limit 1))))))), 0) as id_cargo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'responsavel', p.cpf))) as id_gerado
	from (
		-- Responsáveis das sessões de julgamento
		(select distinct
			p.unicodigo,
			(regexp_replace(p.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf,
			p.uninomerazao as nome
		from wco.tbintegrante r
		natural join wun.tbunico p
		order by 3)
		union all
		-- Responsáveis de empresas
		(select distinct
			p.unicodigo,
			(regexp_replace(p.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf,
			p.uninomerazao
		from wun.tbunicojuridica pj
		inner join wun.tbunico p on (pj.unicodigores = p.unicodigo)
		where pj.unicodigores is not null
		order by 3)
	) as p
) as tab
where id_gerado is null
--limit 2