select distinct
       id,
       ano,
       left(((case when nivel = '1' then '' else ano::varchar end) || organograma || '000000000000000'),15)  as numero,
       nivel,
       descricao,
       sigla,
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade,
       --(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','configuracao-organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))), 1))) as configuracao
        799 as configuracao
from (
	select distinct
		'1' as id,
		ano::smallint as ano,
		ano::text as organograma,
		'1' as nivel,
		('Ano ' || ano)::varchar as descricao,
		null as sigla
	from
		generate_series(1990,2020) as ano
	union
	-- Nível 1 - Orgãos
	select distinct
	    concat(cast(organo as text), cast(orgcodigo as text)) as id,
		organo as ano,
		right('00' || cast(orgcodigo as text), 2) as organograma,
		'2' as nivel,
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
		'3' as nivel,
		left(unidade.unddescricao, 60) as descricao,
		null as sigla
	from wun.tbunidade unidade
	-- Nível 3 - Centro de Cursos
	union
	select distinct
	    concat(cast(organo as text), cncclassif) as id,
		organo as ano,
		regexp_replace(cncclassif, '[\.]', '', 'gi') as organograma,
		((SELECT COUNT(*) FROM regexp_matches(cncclassif, '[.]', 'g')) + 1)::varchar as nivel,
		left(cncdescricao, 60) as descricao,
		null as sigla
	from wun.tbcencus
) tab
where nivel::int >= 1
  and LENGTH(organograma) <= 15
  and ano <= 2020
  and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','configuracao-organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))), 1))), left(((case when nivel = '1' then '' else ano::varchar end) || organograma || '000000000000000'),15)))) is null
order by
  ano asc,
  nivel asc