select
	'300' as sistema,
	'organograma' as tipo_registro,
	'708' as chave_dsk1,
	left(organograma || '00000000000000', 14) as chave_dsk2,
	*
from (
	-- Nível 1 - Orgãos
	select distinct
	    concat(cast(organo as text), cast(orgcodigo as text)) as id,
		organo as ano,
		right('00' || cast(orgcodigo as text), 2) as organograma,
		1 as nivel,
		left(orgdescricao, 60) as descricao,
		null as sigla
	from wun.tborgao

	-- Nível 2 - Unidades
	union
	select distinct
	    concat(cast(unidade.organo as text), cast(unidade.undcodigo as text)) as id,
		unidade.organo as ano,
		( select right('00' || cast(orgcodigo as text), 2)
			from wun.tborgao orgao
			where orgao.orgcodigo = unidade.orgcodigo
			and orgao.organo = unidade.organo) || right('000' || cast(unidade.undcodigo as text), 3) as organograma,
		2 as nivel,
		left(unidade.unddescricao, 60) as descricao,
		null as sigla
	from wun.tbunidade unidade

	-- Nível 3 - Centro de Cursos
	union
	select
	    concat(cast(organo as text), cncclassif) as id,
		organo as ano,
		regexp_replace(cncclassif, '[\.]', '', 'gi') as organograma,
		(SELECT COUNT(*) FROM regexp_matches(cncclassif, '[.]', 'g')) + 1 as nivel,
		left(cncdescricao, 60) as descricao,
		null as sigla
	from wun.tbcencus
) tab
where ano = 2020 and nivel > 3
and COALESCE((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','organograma', '708', left(left(organograma || '00000000000000', 14) || '00000000000000', 14)))), 0) = 0
order by nivel, ano, organograma asc